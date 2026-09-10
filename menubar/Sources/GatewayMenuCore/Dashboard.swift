import Foundation

public struct UsageWindow: Decodable {
    public let name: String
    public let usedPct: Double
    public let resetsAt: String?
}
public struct UsageRow: Decodable {
    public let id: String
    public let name: String
    public let billing: String?
    public let models: [String]
    public let windows: [UsageWindow]
    public let state: String
    public let updatedAt: String?
    public let modelStates: [String: String]?
    public let fableResetAt: String?

    public func window(_ name: String) -> UsageWindow? { windows.first { $0.name == name } }
    public var minimumRemaining: String {
        MenuFormat.remaining(windows.map(\.usedPct).max())
    }
    public var details: String {
        var parts = [name, "Models: " + models.joined(separator: ", "), "State: " + state]
        if let updatedAt { parts.append("Usage updated: " + updatedAt) }
        if let fableResetAt { parts.append("Fable limit resets: " + MenuFormat.reset(fableResetAt, weekly: true)) }
        for (model, state) in (modelStates ?? [:]).sorted(by: { $0.key < $1.key }) { parts.append(model + ": " + state) }
        return parts.joined(separator: "\n")
    }
}
public struct CurrentFlow: Decodable {
    public let at: String
    public let route: String
    public let model: String
    public let upstream: String
    public let headerMs: Double
    public let tps: Double
    public let fallback: Bool
}
public struct TrafficRow: Decodable {
    public let route: String
    public let ok: Int
    public let errors: Int
    public let fallbacks: Int
    public let tps: Double
}
public struct Dashboard: Decodable {
    public let version: Int
    public let generatedAt: String
    public let activeRequests: Int
    public let draining: Bool
    public let networkState: String
    public let accounts: [UsageRow]
    public let providers: [UsageRow]
    public let current: CurrentFlow?
    public let traffic: [TrafficRow]
    public let trafficMinutes: Int
    public let trafficLimited: Bool

    public static func decode(_ data: Data, now: Date = Date()) throws -> Dashboard {
        let value = try JSONDecoder().decode(Dashboard.self, from: data)
        guard value.version == 1 else { throw MenuError.incompatible }
        guard let at = MenuFormat.date(value.generatedAt), abs(now.timeIntervalSince(at)) < 120,
              (value.accounts + value.providers).allSatisfy({ row in row.windows.allSatisfy { (0...100).contains($0.usedPct) } }) else { throw MenuError.response }
        return value
    }
}
public enum MenuFormat {
    public static func statusTitle(_ route: String) -> String {
        let provider = (route.components(separatedBy: " / ").first ?? route).trimmingCharacters(in: .whitespacesAndNewlines)
        let name = provider == "Anthropic" ? "Claude" : provider
        guard !name.isEmpty else { return "AI" }
        return "AI " + (name.count > 12 ? String(name.prefix(11)) + "…" : name)
    }
    public static func date(_ raw: String?) -> Date? {
        guard let raw else { return nil }
        let f = ISO8601DateFormatter()
        f.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return f.date(from: raw) ?? ISO8601DateFormatter().date(from: raw)
    }
    public static func remaining(_ used: Double?) -> String {
        guard let used, used.isFinite, (0...100).contains(used) else { return "—" }
        return String(format: "%.0f%%", 100 - used)
    }
    public static func reset(_ raw: String?, weekly: Bool = false, zone: TimeZone = .autoupdatingCurrent, locale: Locale = .autoupdatingCurrent) -> String {
        guard let date = date(raw), date.timeIntervalSince1970 > 0 else { return "—" }
        let f = DateFormatter()
        f.locale = locale
        f.timeZone = zone
        f.setLocalizedDateFormatFromTemplate(weekly ? "EEEjm" : "jm")
        return f.string(from: date)
    }
    public static func age(_ raw: String, now: Date = Date()) -> String {
        guard let date = date(raw) else { return "unknown age" }
        let seconds = max(0, Int(now.timeIntervalSince(date)))
        return seconds < 60 ? "\(seconds)s ago" : "\(seconds / 60)m ago"
    }
}
