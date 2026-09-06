import AppKit
import GatewayMenuCore

final class GatewayMenuApp: NSObject, NSApplicationDelegate, NSMenuDelegate {
    private var item: NSStatusItem!
    private var timer: Timer?
    private var client: DashboardClient?
    private var configuration = MenuConfiguration()
    private var snapshot: Dashboard?
    private var problem: String?
    private var fetching = false
    private var configurationValid = false
    private var pendingReload = false
    private let configURL: URL
    private let defaultConfig: Bool

    init(configURL: URL, defaultConfig: Bool) { self.configURL = configURL; self.defaultConfig = defaultConfig }

    func applicationDidFinishLaunching(_ notification: Notification) {
        if let id = Bundle.main.bundleIdentifier {
            let current = ProcessInfo.processInfo.processIdentifier
            if NSRunningApplication.runningApplications(withBundleIdentifier: id).contains(where: { $0.processIdentifier < current && !$0.isTerminated }) {
                NSApp.terminate(nil); return
            }
        }
        NSApp.setActivationPolicy(.accessory)
        item = NSStatusBar.system.statusItem(withLength: NSStatusItem.variableLength)
        item.button?.title = "AI…"
        let menu = NSMenu()
        menu.delegate = self
        item.menu = menu
        reloadConfiguration()
    }

    @objc private func reloadConfiguration() {
        guard !fetching else { pendingReload = true; return }
        do {
            let next = try MenuConfiguration.load(configURL, allowMissing: defaultConfig)
            configuration = next
            configurationValid = true
            snapshot = nil
            problem = nil
            timer?.invalidate()
            timer = Timer.scheduledTimer(withTimeInterval: next.refreshSeconds, repeats: true) { [weak self] _ in self?.refresh() }
            // Timers also run while a menu is tracking.
            if let timer { RunLoop.main.add(timer, forMode: .common) }
            refresh()
        } catch {
            configurationValid = false
            timer?.invalidate()
            snapshot = nil
            problem = "Configuration error. Open menu configuration and check its values."
            rebuild()
        }
    }

    @objc private func refresh() {
        guard configurationValid, !fetching else { return }
        fetching = true
        let client = DashboardClient()
        self.client = client
        client.fetch(configuration) { [weak self] result in
            DispatchQueue.main.async {
                guard let self else { return }
                self.fetching = false
                self.client = nil
                if self.pendingReload { self.pendingReload = false; self.reloadConfiguration(); return }
                switch result {
                case .success(let value): self.snapshot = value; self.problem = nil
                case .failure(let error):
                    self.snapshot = nil // Never display old quota as current after a failed poll.
                    self.problem = (error as? MenuError)?.localizedDescription ?? "Gateway unavailable. Retrying automatically."
                }
                self.rebuild()
            }
        }
    }

    func menuWillOpen(_ menu: NSMenu) { rebuild(); refresh() }

    private func rebuild() {
        guard let menu = item?.menu else { return }
        menu.removeAllItems()
        item.button?.title = snapshot.map { $0.draining ? "AI Draining" : ($0.current.map { MenuFormat.statusTitle($0.route) } ?? "AI Idle") } ?? "AI Offline"
        item.button?.toolTip = (snapshot?.current?.route).map { $0 + "\n" + configuration.gatewayURL } ?? "Claude Gateway · " + configuration.gatewayURL
        section(menu, "Claude Gateway · " + configuration.gatewayURL)
        if let problem { text(menu, problem) }
        if let value = snapshot {
            text(menu, "\(value.activeRequests) active · updated \(MenuFormat.age(value.generatedAt))" + (value.draining ? " · draining" : ""))
            if value.networkState != "normal" { text(menu, "Shared network: " + value.networkState) }
            if let current = value.current {
                section(menu, "Latest completed response")
                text(menu, current.model + " → " + current.route)
                text(menu, current.upstream)
                text(menu, "\(Int(current.headerMs)) ms TTFB · \(rate(current.tps)) tok/s · \(MenuFormat.age(current.at))" + (current.fallback ? " · fallback" : ""))
            }
            let accounts = configuration.ordered(value.accounts, accounts: true)
            if !accounts.isEmpty {
                section(menu, "Claude accounts · remaining usage")
                let widths: [CGFloat] = [145, 128, 56, 80, 56, 110, 94]
                grid(menu, ["ACCOUNT", "MODELS", "5H LEFT", "RESET AT", "WK LEFT", "WK RESET", "STATE"], widths, header: true)
                for row in accounts {
                    grid(menu, [row.name, row.models.joined(separator: "/"), remaining(row, "fiveHour"), MenuFormat.reset(row.window("fiveHour")?.resetsAt), remaining(row, "weekly"), MenuFormat.reset(row.window("weekly")?.resetsAt, weekly: true), row.state], widths, details: row.details)
                }
            }
            let providers = configuration.ordered(value.providers, accounts: false)
            if !providers.isEmpty {
                section(menu, configuration.showMeteredProviders ? "Other providers · remaining usage" : "Other subscriptions · remaining usage")
                let widths: [CGFloat] = [145, 160, 56, 56, 56, 66, 94]
                grid(menu, ["PROVIDER", "MODELS", "5H LEFT", "WK LEFT", "MO LEFT", "MIN LEFT", "STATE"], widths, header: true)
                for row in providers {
                    grid(menu, [row.name, row.models.joined(separator: "/"), remaining(row,"fiveHour"), remaining(row,"weekly"), remaining(row,"monthly"), row.minimumRemaining, row.state], widths, details: row.details)
                }
            }
            section(menu, "Traffic · last \(value.trafficMinutes) min" + (value.trafficLimited ? " · retained sample" : ""))
            text(menu, "Completed responses and failed upstream attempts; excludes policy skips.")
            let widths: [CGFloat] = [280, 58, 58, 58, 58]
            grid(menu, ["ROUTE", "OK", "ERR", "TPS", "FB"], widths, header: true, numericFrom: 1)
            let rows = value.traffic.sorted { $0.ok + $0.errors > $1.ok + $1.errors }
            for row in rows.prefix(configuration.maxTrafficRows) {
                grid(menu, [row.route, "\(row.ok)", "\(row.errors)", rate(row.tps), "\(row.fallbacks)"], widths, numericFrom: 1)
            }
            if rows.isEmpty { text(menu, "No recent traffic") }
            if rows.count > configuration.maxTrafficRows { text(menu,"\(rows.count-configuration.maxTrafficRows) more routes in dashboard JSON") }
            text(menu, "Unknown = no quota telemetry. Stale = previous reading. Hover rows for model details.")
        }
        menu.addItem(.separator())
        action(menu, "Refresh display", #selector(refresh), "r")
        action(menu, "Reload menu configuration", #selector(reloadConfiguration))
        action(menu, "Open menu configuration", #selector(openConfiguration))
        action(menu, "Open dashboard JSON", #selector(openDashboard))
        menu.addItem(.separator())
        action(menu, "Quit Claude Gateway Menu", #selector(quit), "q")
    }
    private func remaining(_ row: UsageRow, _ name: String) -> String { MenuFormat.remaining(row.window(name)?.usedPct) }
    private func rate(_ value: Double) -> String { value > 0 ? String(format: "%.0f", value) : "—" }
    private func text(_ menu: NSMenu, _ title: String) {
        let row = NSMenuItem(title: title, action: nil, keyEquivalent: "")
        row.isEnabled = false
        menu.addItem(row)
    }
    private func section(_ menu: NSMenu, _ title: String) {
        if !menu.items.isEmpty { menu.addItem(.separator()) }
        text(menu,title)
    }
    private func grid(_ menu: NSMenu, _ values: [String], _ widths: [CGFloat], header: Bool = false, details: String? = nil, numericFrom: Int = 2) {
        let gap: CGFloat = 12, padding: CGFloat = 18, height: CGFloat = 24
        let view = NSView(frame: NSRect(x: 0, y: 0, width: widths.reduce(0,+) + gap*CGFloat(widths.count-1) + padding*2, height: height))
        view.toolTip = details
        var x = padding
        for (index, value) in values.enumerated() {
            let label = NSTextField(labelWithString: value)
            label.font = .systemFont(ofSize: header ? 11 : 13, weight: header ? .semibold : .regular)
            label.textColor = header ? .secondaryLabelColor : color(value)
            label.alignment = index >= numericFrom && (numericFrom == 1 || index < values.count-1) ? .right : .left
            label.lineBreakMode = .byTruncatingTail
            label.maximumNumberOfLines = 1
            label.frame = NSRect(x:x,y:3,width:widths[index],height:18)
            view.addSubview(label)
            x += widths[index]+gap
        }
        let row = NSMenuItem()
        row.view = view
        row.toolTip = details
        menu.addItem(row)
    }
    private func color(_ value: String) -> NSColor {
        switch value {
        case "OK": return .systemGreen
        case "LOW", "STALE", "RESERVE", "RESTRICTED", "DEGRADED": return .systemOrange
        case "FULL", "AUTH", "BLOCKED": return .systemRed
        case "UNKNOWN", "—": return .secondaryLabelColor
        default: return .labelColor
        }
    }
    private func action(_ menu: NSMenu, _ title: String, _ selector: Selector, _ key: String = "") {
        let row = NSMenuItem(title:title,action:selector,keyEquivalent:key)
        row.target = self
        menu.addItem(row)
    }
    @objc private func openConfiguration() {
        if FileManager.default.fileExists(atPath: configURL.path) { NSWorkspace.shared.open(configURL) }
        else { problem = "Create menubar.json using the documented example, then reload configuration."; rebuild() }
    }
    @objc private func openDashboard() { if configurationValid, let url = try? configuration.endpoint { NSWorkspace.shared.open(url) } }
    @objc private func quit() { NSApp.terminate(nil) }
}

let args = Array(CommandLine.arguments.dropFirst())
var configURL = MenuConfiguration.defaultURL
var defaultConfig = true
var check = false
var index = 0
while index < args.count {
    switch args[index] {
    case "--config":
        guard index+1 < args.count else { fputs("--config needs a path\n", stderr); exit(2) }
        configURL = URL(fileURLWithPath: NSString(string:args[index+1]).expandingTildeInPath)
        defaultConfig = false
        index += 2
    case "--check": check = true; index += 1
    default: fputs("Usage: GatewayMenu [--config PATH] [--check]\n", stderr); exit(2)
    }
}
if check {
    do {
        let config = try MenuConfiguration.load(configURL, allowMissing: defaultConfig)
        let client = DashboardClient(), done = DispatchSemaphore(value:0)
        var success = false
        client.fetch(config) { result in
            switch result {
            case .success(let value): print("Dashboard v\(value.version): \(value.accounts.count) accounts, \(value.providers.count) providers"); success = true
            case .failure: fputs("Dashboard check failed\n", stderr)
            }
            done.signal()
        }
        guard done.wait(timeout:.now()+10) == .success else { exit(1) }
        exit(success ? 0 : 1)
    } catch { fputs("Menu configuration invalid\n", stderr); exit(2) }
}
let app = NSApplication.shared
let delegate = GatewayMenuApp(configURL:configURL,defaultConfig:defaultConfig)
app.delegate = delegate
app.run()
