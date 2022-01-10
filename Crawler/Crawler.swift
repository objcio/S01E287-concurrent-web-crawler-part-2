//
//  Crawler.swift
//  Crawler
//
//  Created by Chris Eidhof on 21.12.21.
//

import Foundation

actor Queue {
    var items: Set<URL> = []
    var inProgress: Set<URL> = []
    
    func dequeue() -> URL? {
        guard let result = items.popFirst() else { return nil }
        inProgress.insert(result)
        return result
    }
    
    func finish(_ item: URL) {
        inProgress.remove(item)
    }
    
    var done: Bool {
        items.isEmpty && inProgress.isEmpty
    }
    
    func add(newItems: [URL]) {
        items.formUnion(newItems)
    }
}

@MainActor
final class Crawler: ObservableObject {
    @Published var state: [URL: Page] = [:]
    
    func add(_ page: Page) {
        state[page.url] = page
    }
    
    func seenURLs() -> Set<URL> {
        Set(state.keys)
    }
    
    func crawl(url: URL, numberOfWorkers: Int = 4) async throws {
        let basePrefix = url.absoluteString
        let queue = Queue()
        await queue.add(newItems: [url])
        await withThrowingTaskGroup(of: Void.self) { group in
            for i in 0..<numberOfWorkers {
                group.addTask {
                    var numberOfJobs = 0
                    while await !queue.done {
                        guard let job = await queue.dequeue() else {
                            try await Task.sleep(nanoseconds: 100)
                            continue
                        }
                        let page = try await URLSession.shared.page(from: job)
                        let seen = await self.seenURLs()
                        let newURLs = page.outgoingLinks.filter { url in
                            url.absoluteString.hasPrefix(basePrefix) && !seen.contains(url)
                        }
                        await queue.add(newItems: newURLs)
                        await self.add(page)
                        await queue.finish(page.url)
                        numberOfJobs += 1
                    }
                    print("Worker \(i) did \(numberOfJobs) jobs")
                }
            }
        }
       
    }
}

extension URLSession {
    func page(from url: URL) async throws -> Page {
        let (data, _) = try await data(from: url)
        let doc = try XMLDocument(data: data, options: .documentTidyHTML)
        let title = try doc.nodes(forXPath: "//title").first?.stringValue
        let links: [URL] = try doc.nodes(forXPath: "//a[@href]").compactMap { node in
            guard let el = node as? XMLElement else { return nil }
            guard let href = el.attribute(forName: "href")?.stringValue else { return nil }
            return URL(string: href, relativeTo: url)?.simplified
        }
        return Page(url: url, title: title ?? "", outgoingLinks: links)
    }
}

extension URL {
    var simplified: URL {
        var result = absoluteString
        if let i = result.lastIndex(of: "#") {
            result = String(result[..<i])
        }
        if result.last == "/" {
            result.removeLast()
        }
        return URL(string: result)!
    }
}

extension URL: @unchecked Sendable { }

struct Page {
    var url: URL
    var title: String
    var outgoingLinks: [URL]
}
