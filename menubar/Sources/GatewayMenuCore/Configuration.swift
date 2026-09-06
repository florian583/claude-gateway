import Foundation

public struct MenuConfiguration: Decodable {
    public var version = 1
    public var gatewayURL = "http://127.0.0.1:48104"
    public var refreshSeconds: Double = 15
    public var showMeteredProviders = false
    public var accountOrder: [String] = []
    public var providerOrder: [String] = []
    public var maxTrafficRows = 8

    enum CodingKeys: String, CodingKey, CaseIterable {
        case version, gatewayURL, refreshSeconds, showMeteredProviders, accountOrder, providerOrder, maxTrafficRows
    }

    public init() {}
    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        version = try c.decodeIfPresent(Int.self, forKey: .version) ?? 1
        gatewayURL = try c.decodeIfPresent(String.self, forKey: .gatewayURL) ?? gatewayURL
        refreshSeconds = try c.decodeIfPresent(Double.self, forKey: .refreshSeconds) ?? refreshSeconds
        showMeteredProviders = try c.decodeIfPresent(Bool.self, forKey: .showMeteredProviders) ?? false
        accountOrder = try c.decodeIfPresent([String].self, forKey: .accountOrder) ?? []
        providerOrder = try c.decodeIfPresent([String].self, forKey: .providerOrder) ?? []
        maxTrafficRows = try c.decodeIfPresent(Int.self, forKey: .maxTrafficRows) ?? 8
    }

    public static var defaultURL: URL {
        FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent(".config/claude-proxy/menubar.json")
    }

    public static func load(_ url: URL, allowMissing: Bool = false) throws -> MenuConfiguration {
        do { return try parse(Data(contentsOf: url)) }
        catch let error as NSError where allowMissing && error.domain == NSCocoaErrorDomain && error.code == NSFileReadNoSuchFileError {
            return MenuConfiguration()
        }
    }

    public static func parse(_ data: Data) throws -> MenuConfiguration {
        guard data.count <= 64 * 1024,
              let object = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              Set(object.keys).isSubset(of: Set(CodingKeys.allCases.map(\.rawValue))) else {
            throw MenuError.configuration
        }
        let config = try JSONDecoder().decode(MenuConfiguration.self, from: data)
        guard config.version == 1, (5...300).contains(config.refreshSeconds), (1...30).contains(config.maxTrafficRows),
              Set(config.accountOrder).count == config.accountOrder.count,
              Set(config.providerOrder).count == config.providerOrder.count else { throw MenuError.configuration }
        _ = try config.endpoint
        return config
    }

    public var endpoint: URL {
        get throws {
            guard let c = URLComponents(string: gatewayURL),
                  ["http", "https"].contains(c.scheme ?? ""),
                  ["127.0.0.1", "[::1]", "::1", "localhost"].contains(c.host ?? ""),
                  c.user == nil, c.password == nil, c.query == nil, c.fragment == nil,
                  c.path.isEmpty || c.path == "/", (1...65535).contains(c.port ?? 80),
                  let url = c.url else { throw MenuError.configuration }
            return url.appendingPathComponent("dashboard")
        }
    }

    public func ordered(_ rows: [UsageRow], accounts: Bool) -> [UsageRow] {
        let order = accounts ? accountOrder : providerOrder
        return rows.filter { accounts || showMeteredProviders || $0.billing != "metered" }.sorted {
            let a = order.firstIndex(of: $0.id) ?? Int.max
            let b = order.firstIndex(of: $1.id) ?? Int.max
            return a == b ? $0.id < $1.id : a < b
        }
    }
}

public enum MenuError: Error, LocalizedError {
    case configuration, incompatible, response, oversized, redirect
    public var errorDescription: String? {
        switch self {
        case .configuration: return "Invalid menu configuration. Check URL, keys, version, and limits."
        case .incompatible: return "Incompatible gateway dashboard. Update gateway and menu together."
        case .response: return "Gateway unavailable or returned invalid data."
        case .oversized: return "Gateway response exceeds 1 MiB."
        case .redirect: return "Gateway redirects are not allowed."
        }
    }
}
