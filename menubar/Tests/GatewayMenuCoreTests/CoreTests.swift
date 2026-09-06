import XCTest
@testable import GatewayMenuCore

final class CoreTests: XCTestCase {
    private func data(_ json: String) -> Data { Data(json.utf8) }
    func testConfigurationDefaultsAndOverrides() throws {
        let defaults = try MenuConfiguration.parse(data("{}"))
        XCTAssertEqual(try defaults.endpoint.absoluteString,"http://127.0.0.1:48104/dashboard")
        XCTAssertFalse(defaults.showMeteredProviders)
        let custom = try MenuConfiguration.parse(data(#"{"gatewayURL":"http://[::1]:49999","refreshSeconds":60,"accountOrder":["backup","work"]}"#))
        XCTAssertEqual(try custom.endpoint.port,49999)
        XCTAssertEqual(custom.refreshSeconds,60)
        XCTAssertEqual(custom.accountOrder,["backup","work"])
    }
    func testRejectsUnsafeAndMistypedConfiguration() {
        for json in [#"{"gatewayURL":"https://example.com"}"#, #"{"gatewayURL":"http://localhost.evil.test"}"#, #"{"gatewayURL":"http://user:pass@127.0.0.1"}"#, #"{"gatewayURL":"http://127.0.0.1/config?key=value"}"#, #"{"refreshSeconds":0}"#, #"{"version":2}"#, #"{"refeshSeconds":15}"#, #"{"accountOrder":["work","work"]}"#, #"{"maxTrafficRows":999}"#, #"{"showMeteredProviders":"true"}"#] {
            XCTAssertThrowsError(try MenuConfiguration.parse(data(json)),json)
        }
    }
    func testMissingConfigOnlyOptionalAtDefaultLocation() throws {
        let path = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString).appendingPathComponent("missing.json")
        XCTAssertNoThrow(try MenuConfiguration.load(path,allowMissing:true))
        XCTAssertThrowsError(try MenuConfiguration.load(path))
    }
    func testResetsUseProvidedTimezoneAndLocaleWithoutRounding() {
        let utc=TimeZone(secondsFromGMT:0)!, singapore=TimeZone(secondsFromGMT:8*3600)!, locale=Locale(identifier:"en_US_POSIX")
        let raw="2026-09-06T05:00:59Z"
        XCTAssertTrue(MenuFormat.reset(raw,zone:utc,locale:locale).contains("5:00"))
        XCTAssertTrue(MenuFormat.reset(raw,zone:singapore,locale:locale).contains("1:00"))
        XCTAssertTrue(MenuFormat.reset(raw,weekly:true,zone:utc,locale:locale).contains("Sun"))
        XCTAssertEqual(MenuFormat.reset(nil),"—")
        XCTAssertEqual(MenuFormat.remaining(nil),"—")
        XCTAssertEqual(MenuFormat.remaining(100),"0%")
        XCTAssertEqual(MenuFormat.remaining(.nan),"—")
        XCTAssertNotNil(MenuFormat.date("2026-09-06T05:00:00.123456789Z"))
    }
    func testDashboardContractAndUnknownUsage() throws {
        let raw=#"{"version":1,"generatedAt":"2026-09-06T05:00:00Z","activeRequests":2,"draining":false,"networkState":"normal","accounts":[{"id":"work","name":"Work account","models":["test-model"],"windows":[],"state":"UNKNOWN"}],"providers":[{"id":"paid","name":"Paid API","models":[],"windows":[],"state":"UNKNOWN","billing":"metered"}],"traffic":[],"trafficMinutes":30,"trafficLimited":true}"#
        let now=MenuFormat.date("2026-09-06T05:00:05Z")!
        let snapshot=try Dashboard.decode(data(raw),now:now)
        XCTAssertNil(snapshot.current)
        XCTAssertEqual(snapshot.accounts[0].minimumRemaining,"—")
        XCTAssertTrue(snapshot.trafficLimited)
        XCTAssertTrue(MenuConfiguration().ordered(snapshot.providers,accounts:false).isEmpty)
        XCTAssertEqual(MenuConfiguration().ordered(snapshot.accounts,accounts:true).first?.name,"Work account")
        XCTAssertThrowsError(try Dashboard.decode(data(raw),now:now.addingTimeInterval(300)))
        XCTAssertThrowsError(try Dashboard.decode(data(raw.replacingOccurrences(of:"\"version\":1",with:"\"version\":2")),now:now))
    }
}
