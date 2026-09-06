import XCTest
@testable import GatewayMenuCore

private final class MockDashboardProtocol: URLProtocol {
    static var handler: ((URLRequest) -> (Int, Data))?
    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
    override func startLoading() {
        let (status, data) = Self.handler!(request)
        let response = HTTPURLResponse(url:request.url!,statusCode:status,httpVersion:nil,headerFields:["Content-Type":"application/json"])!
        client?.urlProtocol(self,didReceive:response,cacheStoragePolicy:.notAllowed)
        client?.urlProtocol(self,didLoad:data)
        client?.urlProtocolDidFinishLoading(self)
    }
    override func stopLoading() {}
}

final class ClientTests: XCTestCase {
    func testBoundedReadOnlyRequestAndReuseAfterFailure() {
        let client = DashboardClient(protocolClasses:[MockDashboardProtocol.self])
        for responseSize in [4, DashboardClient.maximumBytes+1] {
            let done=expectation(description:"failed response")
            MockDashboardProtocol.handler = { request in
                XCTAssertEqual(request.httpMethod,"GET")
                XCTAssertEqual(request.url?.path,"/dashboard")
                XCTAssertNil(request.httpBody)
                XCTAssertNil(request.value(forHTTPHeaderField:"Authorization"))
                return (200,Data(repeating:65,count:responseSize))
            }
            XCTAssertTrue(client.fetch(MenuConfiguration()) { result in
                guard case .failure = result else { XCTFail("invalid response accepted");done.fulfill();return }
                done.fulfill()
            })
            wait(for:[done],timeout:3)
        }
    }
    func testOldGatewayIsExplicitlyIncompatible() {
        let done=expectation(description:"old API")
        MockDashboardProtocol.handler = { _ in (404,Data()) }
        let client=DashboardClient(protocolClasses:[MockDashboardProtocol.self])
        client.fetch(MenuConfiguration()) { result in
            guard case .failure(let error) = result, case .incompatible = error as? MenuError else { XCTFail("wrong error");done.fulfill();return }
            done.fulfill()
        }
        wait(for:[done],timeout:3)
    }
}
