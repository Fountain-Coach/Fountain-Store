
//
//  FTS.swift
//  FountainFTS
//
//  Basic inverted index for optional full‑text search module.
//  Tokenization is whitespace and punctuation based. Scoring/BM25 to follow.

import Foundation

public struct FTSIndex: Sendable, Hashable {
    private var postings: [String: Set<String>] = [:]
    private var docTokens: [String: Set<String>] = [:]

    public init() {}

    public mutating func add(docID: String, text: String) {
        let tokens = tokenize(text)
        docTokens[docID] = tokens
        for t in tokens {
            postings[t, default: []].insert(docID)
        }
    }

    public mutating func remove(docID: String) {
        guard let tokens = docTokens.removeValue(forKey: docID) else { return }
        for t in tokens {
            postings[t]?.remove(docID)
            if postings[t]?.isEmpty == true { postings[t] = nil }
        }
    }

    public func search(_ query: String) -> [String] {
        let tokens = Array(tokenize(query))
        guard let first = tokens.first else { return [] }
        var result = postings[first] ?? []
        for t in tokens.dropFirst() {
            result.formIntersection(postings[t] ?? [])
            if result.isEmpty { break }
        }
        return Array(result)
    }

    private func tokenize(_ text: String) -> Set<String> {
        let separators = CharacterSet.alphanumerics.inverted
        let parts = text.lowercased().components(separatedBy: separators)
        return Set(parts.filter { !$0.isEmpty })
    }

    public static func == (lhs: FTSIndex, rhs: FTSIndex) -> Bool {
        lhs.postings == rhs.postings
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(postings.count)
        for (token, ids) in postings.sorted(by: { $0.key < $1.key }) {
            hasher.combine(token)
            hasher.combine(ids.count)
            for id in ids.sorted() { hasher.combine(id) }
        }
    }
}
