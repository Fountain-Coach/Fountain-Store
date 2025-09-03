
//
//  FTS.swift
//  FountainFTS
//
//  Basic inverted index for optional full‑text search module.
//  Tokenization is whitespace and punctuation based with BM25 ranking.

import Foundation

public struct FTSIndex: Sendable, Hashable {
    private var postings: [String: [String: Int]] = [:] // token -> docID -> frequency
    private var docTokens: [String: [String: Int]] = [:] // docID -> token -> frequency
    private var docLengths: [String: Int] = [:] // docID -> token count

    public init() {}

    public mutating func add(docID: String, text: String) {
        let tokens = tokenize(text)
        var freqs: [String: Int] = [:]
        freqs.reserveCapacity(tokens.count)
        for t in tokens { freqs[t, default: 0] += 1 }
        docTokens[docID] = freqs
        docLengths[docID] = tokens.count
        for (t, c) in freqs {
            var docMap = postings[t] ?? [:]
            docMap[docID] = c
            postings[t] = docMap
        }
    }

    public mutating func remove(docID: String) {
        guard let tokens = docTokens.removeValue(forKey: docID) else { return }
        docLengths.removeValue(forKey: docID)
        for (t, _) in tokens {
            postings[t]?.removeValue(forKey: docID)
            if postings[t]?.isEmpty == true { postings[t] = nil }
        }
    }

    public func search(_ query: String) -> [String] {
        let tokens = tokenize(query)
        guard let first = tokens.first else { return [] }
        var result = postings[first].map { Set($0.keys) } ?? Set<String>()
        for t in tokens.dropFirst() {
            let docs = postings[t].map { Set($0.keys) } ?? Set<String>()
            result.formIntersection(docs)
            if result.isEmpty { break }
        }
        var scored: [(String, Double)] = []
        scored.reserveCapacity(result.count)
        for doc in result {
            scored.append((doc, bm25(tokens, doc)))
        }
        scored.sort { $0.1 > $1.1 }
        return scored.map { $0.0 }
    }

    private func tokenize(_ text: String) -> [String] {
        let separators = CharacterSet.alphanumerics.inverted
        let parts = text.lowercased().components(separatedBy: separators)
        return parts.filter { !$0.isEmpty }
    }

    private func bm25(_ queryTokens: [String], _ docID: String) -> Double {
        let k1 = 1.5
        let b = 0.75
        guard let docTerms = docTokens[docID], let docLen = docLengths[docID] else { return 0 }
        let N = docTokens.count
        let avgdl = Double(docLengths.values.reduce(0, +)) / Double(max(1, N))
        var score = 0.0
        for token in queryTokens {
            guard let tf = docTerms[token], let df = postings[token]?.count else { continue }
            let idf = log((Double(N - df) + 0.5) / (Double(df) + 0.5) + 1.0)
            let denom = Double(tf) + k1 * (1 - b + b * Double(docLen) / avgdl)
            score += idf * (Double(tf) * (k1 + 1)) / denom
        }
        return score
    }

    public static func == (lhs: FTSIndex, rhs: FTSIndex) -> Bool {
        lhs.postings == rhs.postings && lhs.docLengths == rhs.docLengths
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(postings.count)
        for (token, docs) in postings.sorted(by: { $0.key < $1.key }) {
            hasher.combine(token)
            hasher.combine(docs.count)
            for (doc, freq) in docs.sorted(by: { $0.key < $1.key }) {
                hasher.combine(doc)
                hasher.combine(freq)
            }
        }
    }
}
