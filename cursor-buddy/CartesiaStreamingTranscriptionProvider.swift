//
//  CartesiaStreamingTranscriptionProvider.swift
//  cursor-buddy
//
//  Streaming transcription provider backed by Cartesia's Ink realtime STT
//  API (`wss://api.cartesia.ai/stt/turns/websocket`, model `ink-2`).
//  Verified against https://docs.cartesia.ai/api-reference/stt/turns/websocket.
//  Uses the same CARTESIA_API_KEY as the TTS client, so one key covers
//  both mic input (Ink) and spoken replies (Sonic).
//

import AVFoundation
import OCAudioCore
import Foundation

struct CartesiaStreamingTranscriptionProviderError: LocalizedError {
    let message: String

    var errorDescription: String? {
        message
    }
}

final class CartesiaStreamingTranscriptionProvider: BuddyTranscriptionProvider {
    private let apiKey = AppBundleConfiguration.cartesiaAPIKey()

    let displayName = "Cartesia"
    let requiresSpeechRecognitionPermission = false

    var isConfigured: Bool {
        apiKey != nil
    }

    var unavailableExplanation: String? {
        guard !isConfigured else { return nil }
        return "Cartesia streaming is not configured. Add a Cartesia API key."
    }

    private let sharedWebSocketURLSession = URLSession(configuration: .default)

    func startStreamingSession(
        keyterms: [String],
        onTranscriptUpdate: @escaping (String) -> Void,
        onFinalTranscriptReady: @escaping (String) -> Void,
        onError: @escaping (Error) -> Void
    ) async throws -> any BuddyStreamingTranscriptionSession {
        guard let apiKey else {
            throw CartesiaStreamingTranscriptionProviderError(
                message: unavailableExplanation ?? "Cartesia streaming is not configured."
            )
        }

        let session = CartesiaStreamingTranscriptionSession(
            apiKey: apiKey,
            urlSession: sharedWebSocketURLSession,
            keyterms: keyterms,
            onTranscriptUpdate: onTranscriptUpdate,
            onFinalTranscriptReady: onFinalTranscriptReady,
            onError: onError
        )

        try await session.open()
        return session
    }
}

private nonisolated final class CartesiaStreamingTranscriptionSession: StreamingWebSocketTranscriptionSession, @unchecked Sendable, BuddyStreamingTranscriptionSession {
    private struct MessageEnvelope: Decodable {
        let type: String?
    }

    private struct TurnMessage: Decodable {
        let type: String?
        let transcript: String?
    }

    private struct ErrorMessage: Decodable {
        let type: String?
        let title: String?
        let message: String?
    }

    private static let websocketBaseURLString = "wss://api.cartesia.ai/stt/turns/websocket"
    private static let modelID = "ink-2"
    private static let cartesiaVersion = "2026-08-14"
    private static let targetSampleRate = 16_000.0
    private static let explicitFinalTranscriptGracePeriodSeconds = 1.6

    let finalTranscriptFallbackDelaySeconds: TimeInterval = 3.0

    private let apiKey: String
    private let keyterms: [String]
    private let onTranscriptUpdate: (String) -> Void
    private let onFinalTranscriptReady: (String) -> Void
    private let onError: (Error) -> Void

    private let stateQueue = DispatchQueue(label: "com.jkneen.openclicky.cartesia-stt.state")
    private let audioPCM16Converter = BuddyPCM16AudioConverter(targetSampleRate: targetSampleRate)

    private var audioFramesSent = 0
    private var audioDropCount = 0
    private var hasDeliveredFinalTranscript = false
    private var isAwaitingExplicitFinalTranscript = false
    private var isCancelled = false
    private var latestTurnTranscriptText = ""
    private var explicitFinalTranscriptDeadlineWorkItem: DispatchWorkItem?

    init(
        apiKey: String,
        urlSession: URLSession,
        keyterms: [String],
        onTranscriptUpdate: @escaping (String) -> Void,
        onFinalTranscriptReady: @escaping (String) -> Void,
        onError: @escaping (Error) -> Void
    ) {
        self.apiKey = apiKey
        self.keyterms = keyterms
        self.onTranscriptUpdate = onTranscriptUpdate
        self.onFinalTranscriptReady = onFinalTranscriptReady
        self.onError = onError
        super.init(urlSession: urlSession, sendQueueLabel: "com.jkneen.openclicky.cartesia-stt.send")
    }

    func open() async throws {
        let websocketURL = try Self.makeWebsocketURL(keyterms: keyterms)
        print("[CartesiaSTT] opening WebSocket: \(websocketURL.absoluteString)")
        var websocketRequest = URLRequest(url: websocketURL)
        // STT reference documents `X-API-Key`; the TTS endpoints accept
        // `Authorization: Bearer`. Send both so either scheme is satisfied.
        websocketRequest.setValue(apiKey, forHTTPHeaderField: "X-API-Key")
        websocketRequest.setValue("Bearer \(apiKey)", forHTTPHeaderField: "Authorization")

        openWebSocket(with: websocketRequest)
    }

    func appendAudioBuffer(_ audioBuffer: AVAudioPCMBuffer) {
        guard let audioPCM16Data = audioPCM16Converter.convertToPCM16Data(from: audioBuffer),
              !audioPCM16Data.isEmpty else {
            audioDropCount += 1
            if audioDropCount <= 5 {
                print("[CartesiaSTT] buffer dropped (converter empty #\(audioDropCount)): frames=\(audioBuffer.frameLength) sr=\(audioBuffer.format.sampleRate) ch=\(audioBuffer.format.channelCount)")
            }
            return
        }

        audioFramesSent += 1
        if audioFramesSent == 1 {
            print("[CartesiaSTT] first audio frame sent (\(audioPCM16Data.count) bytes)")
        } else if audioFramesSent % 100 == 0 {
            print("[CartesiaSTT] frames sent: \(audioFramesSent)")
        }
        sendAudioData(audioPCM16Data) { [weak self] error in
            print("[CartesiaSTT] audio send error: \(error.localizedDescription)")
            self?.failSession(with: error)
        }
    }

    func requestFinalTranscript() {
        stateQueue.async {
            guard !self.hasDeliveredFinalTranscript else { return }
            self.isAwaitingExplicitFinalTranscript = true
            self.scheduleExplicitFinalTranscriptDeadline()
        }

        // No finalize command on the turns endpoint — closing processes all
        // buffered audio into turn events, and `turn.end` delivers the final.
        sendJSONMessage(["type": "close"]) { [weak self] error in
            self?.failSession(with: error)
        }
    }

    func cancel() {
        stateQueue.async {
            self.isCancelled = true
            self.explicitFinalTranscriptDeadlineWorkItem?.cancel()
            self.explicitFinalTranscriptDeadlineWorkItem = nil
        }

        sendJSONMessage(["type": "close"]) { [weak self] error in
            self?.failSession(with: error)
        }
        closeWebSocket()
    }

    override func handleReceiveFailure(_ error: Error) {
        let nsError = error as NSError
        let closeCode = webSocketTask?.closeCode.rawValue ?? -1
        print("[CartesiaSTT] receive failure: domain=\(nsError.domain) code=\(nsError.code) closeCode=\(closeCode) — \(error.localizedDescription)")
        failSession(with: error)
    }

    override func handleIncomingText(_ text: String) {
        guard let messageData = text.data(using: .utf8) else { return }

        do {
            let envelope = try JSONDecoder().decode(MessageEnvelope.self, from: messageData)
            switch envelope.type {
            case "connected":
                print("[CartesiaSTT] connected (session alive)")
            case "turn.start":
                print("[CartesiaSTT] turn started")
            case "turn.update", "turn.eager_end":
                let turnMessage = try JSONDecoder().decode(TurnMessage.self, from: messageData)
                handleTurnTranscript(turnMessage.transcript, isFinal: false)
            case "turn.resume":
                print("[CartesiaSTT] turn resumed after eager end")
            case "turn.end":
                let turnMessage = try JSONDecoder().decode(TurnMessage.self, from: messageData)
                handleTurnTranscript(turnMessage.transcript, isFinal: true)
            case "error":
                let errorMessage = try JSONDecoder().decode(ErrorMessage.self, from: messageData)
                let messageText = [errorMessage.title, errorMessage.message]
                    .compactMap { $0?.trimmingCharacters(in: .whitespacesAndNewlines) }
                    .filter { !$0.isEmpty }
                    .joined(separator: ": ")
                print("[CartesiaSTT] server error frame: \(messageText)")
                failSession(with: CartesiaStreamingTranscriptionProviderError(
                    message: messageText.isEmpty ? "Cartesia returned an error." : messageText
                ))
            default:
                print("[CartesiaSTT] unknown frame type: \(envelope.type ?? "nil") — \(text.prefix(200))")
            }
        } catch {
            print("[CartesiaSTT] failed to decode frame: \(error.localizedDescription) — raw=\(text.prefix(200))")
            failSession(with: error)
        }
    }

    private func handleTurnTranscript(_ transcript: String?, isFinal: Bool) {
        let transcriptText = transcript?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""

        stateQueue.async {
            // Ink transcripts are cumulative within a turn and never revised,
            // so the latest text replaces (not appends to) the turn buffer.
            self.latestTurnTranscriptText = transcriptText
            if !transcriptText.isEmpty {
                self.onTranscriptUpdate(transcriptText)
            }

            guard isFinal else { return }
            self.deliverFinalTranscriptIfNeeded(transcriptText)
        }
    }

    private func scheduleExplicitFinalTranscriptDeadline() {
        explicitFinalTranscriptDeadlineWorkItem?.cancel()

        let deadlineWorkItem = DispatchWorkItem { [weak self] in
            self?.stateQueue.async {
                guard let self else { return }
                self.deliverFinalTranscriptIfNeeded(self.latestTurnTranscriptText)
            }
        }

        explicitFinalTranscriptDeadlineWorkItem = deadlineWorkItem
        DispatchQueue.main.asyncAfter(
            deadline: .now() + Self.explicitFinalTranscriptGracePeriodSeconds,
            execute: deadlineWorkItem
        )
    }

    private func deliverFinalTranscriptIfNeeded(_ transcriptText: String) {
        guard !hasDeliveredFinalTranscript else { return }
        hasDeliveredFinalTranscript = true
        explicitFinalTranscriptDeadlineWorkItem?.cancel()
        explicitFinalTranscriptDeadlineWorkItem = nil
        onFinalTranscriptReady(transcriptText)
        sendJSONMessage(["type": "close"]) { [weak self] error in
            self?.failSession(with: error)
        }
    }

    private func failSession(with error: Error) {
        let reportedError = Self.reportedError(for: error)
        stateQueue.async {
            if self.isCancelled {
                print("[CartesiaSTT] post-cancel failure (suppressed): \(reportedError.localizedDescription)")
                return
            }

            // The turns endpoint closes (1000) right after our `close` frame,
            // which surfaces as an ENOTCONN receive failure. That teardown is
            // expected — never report it once we are finalizing or done.
            if self.hasDeliveredFinalTranscript {
                print("[CartesiaSTT] post-final teardown error (suppressed): \(reportedError.localizedDescription)")
                return
            }

            if self.isAwaitingExplicitFinalTranscript {
                print("[CartesiaSTT] teardown during finalization, delivering transcript (length \(self.latestTurnTranscriptText.count)): \(reportedError.localizedDescription)")
                self.deliverFinalTranscriptIfNeeded(self.latestTurnTranscriptText)
                return
            }

            print("[CartesiaSTT] Session failed with error: \(reportedError.localizedDescription)")
            self.onError(reportedError)
        }
    }

    private static func reportedError(for error: Error) -> Error {
        let nsError = error as NSError
        guard nsError.domain == NSURLErrorDomain,
              nsError.code == NSURLErrorBadServerResponse else {
            return error
        }

        return CartesiaStreamingTranscriptionProviderError(
            message: "Cartesia rejected the streaming connection. Check the Cartesia API key and account status."
        )
    }

    private static func makeWebsocketURL(keyterms: [String]) throws -> URL {
        guard var websocketURLComponents = URLComponents(string: websocketBaseURLString) else {
            throw CartesiaStreamingTranscriptionProviderError(message: "Cartesia websocket URL is invalid.")
        }

        var queryItems = [
            URLQueryItem(name: "model", value: modelID),
            URLQueryItem(name: "encoding", value: "pcm_s16le"),
            URLQueryItem(name: "sample_rate", value: String(Int(targetSampleRate))),
            URLQueryItem(name: "cartesia_version", value: cartesiaVersion)
        ]

        for keyterm in keyterms
            .map({ $0.trimmingCharacters(in: .whitespacesAndNewlines) })
            .filter({ !$0.isEmpty })
            .prefix(100) {
            queryItems.append(URLQueryItem(name: "keyterm", value: keyterm))
        }

        websocketURLComponents.queryItems = queryItems

        guard let websocketURL = websocketURLComponents.url else {
            throw CartesiaStreamingTranscriptionProviderError(message: "Cartesia websocket URL could not be created.")
        }

        return websocketURL
    }
}
