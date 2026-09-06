import Foundation

// One bounded GET per poll. No proxy credentials, cookies, disk logs, browser
// telemetry, or redirects. URLSession's delegate queue serializes buffer access.
public final class DashboardClient: NSObject, URLSessionDataDelegate {
    private var session: URLSession?
    private var data = Data()
    private var completion: ((Result<Dashboard, Error>) -> Void)?
    private var task: URLSessionDataTask?
    private let lock = NSLock()
    private let testProtocols: [AnyClass]?
    public static let maximumBytes = 1024 * 1024

    public override init() { testProtocols = nil; super.init() }
    init(protocolClasses: [AnyClass]) { testProtocols = protocolClasses; super.init() }

    @discardableResult
    public func fetch(_ config: MenuConfiguration, completion: @escaping (Result<Dashboard, Error>) -> Void) -> Bool {
        lock.lock()
        guard self.completion == nil else { lock.unlock(); return false }
        self.completion = completion
        lock.unlock()
        do {
            let endpoint = try config.endpoint
            let options = URLSessionConfiguration.ephemeral
            options.timeoutIntervalForRequest = 5
            options.timeoutIntervalForResource = 8
            options.requestCachePolicy = .reloadIgnoringLocalCacheData
            options.httpCookieStorage = nil
            options.urlCredentialStorage = nil
            options.connectionProxyDictionary = [:]
            options.protocolClasses = testProtocols
            let session = URLSession(configuration: options, delegate: self, delegateQueue: nil)
            var request = URLRequest(url: endpoint)
            request.httpMethod = "GET"
            request.setValue("application/json", forHTTPHeaderField: "Accept")
            request.setValue("claude-gateway-menu/1", forHTTPHeaderField: "User-Agent")
            let task = session.dataTask(with: request)
            lock.lock()
            self.data = Data()
            self.session = session
            self.task = task
            lock.unlock()
            task.resume()
        } catch { finish(.failure(error)) }
        return true
    }
    private func finish(_ result: Result<Dashboard, Error>, task expected: URLSessionTask? = nil) {
        lock.lock()
        if let expected, expected !== task { lock.unlock(); return }
        let previousSession = session
        session = nil
        task = nil
        let callback = completion
        completion = nil
        lock.unlock()
        previousSession?.invalidateAndCancel()
        callback?(result)
    }
    private func isCurrent(_ task: URLSessionTask) -> Bool {
        lock.lock(); defer { lock.unlock() }
        return task === self.task
    }
    public func urlSession(_ session: URLSession, task: URLSessionTask, willPerformHTTPRedirection response: HTTPURLResponse, newRequest request: URLRequest, completionHandler: @escaping (URLRequest?) -> Void) {
        completionHandler(nil)
        finish(.failure(MenuError.redirect), task: task)
    }
    public func urlSession(_ session: URLSession, dataTask: URLSessionDataTask, didReceive response: URLResponse, completionHandler: @escaping (URLSession.ResponseDisposition) -> Void) {
        guard isCurrent(dataTask) else { completionHandler(.cancel); return }
        guard let http = response as? HTTPURLResponse, http.statusCode == 200 else {
            completionHandler(.cancel)
            finish(.failure((response as? HTTPURLResponse)?.statusCode == 404 ? MenuError.incompatible : MenuError.response), task: dataTask)
            return
        }
        guard response.expectedContentLength <= Self.maximumBytes else { completionHandler(.cancel); finish(.failure(MenuError.oversized), task: dataTask); return }
        completionHandler(.allow)
    }
    public func urlSession(_ session: URLSession, dataTask: URLSessionDataTask, didReceive data: Data) {
        guard isCurrent(dataTask) else { return }
        guard self.data.count + data.count <= Self.maximumBytes else { finish(.failure(MenuError.oversized), task: dataTask); return }
        self.data.append(data)
    }
    public func urlSession(_ session: URLSession, task: URLSessionTask, didCompleteWithError error: Error?) {
        guard isCurrent(task) else { return }
        if let error { finish(.failure(error), task: task) }
        else { do { finish(.success(try Dashboard.decode(data)), task: task) } catch { finish(.failure(error), task: task) } }
    }
}
