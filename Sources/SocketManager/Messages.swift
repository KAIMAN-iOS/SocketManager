//
//  File.swift
//  
//
//  Created by GG on 23/03/2021.
//

import Foundation
import Starscream
import Network

public typealias SocketRoute = String
/// In order to handle custom message, please sublclass SocketMessage and use your own data
open class SocketBaseMessage: Codable {
    public var id: Int
    /// used when queuing messaging
    ///  if set to true, all previous messages of the same class will be deleted from queue
    public var singleInstance: Bool = false
    
    enum CodingKeys: String, CodingKey {
        case id
    }
    
    init(id: Int = UUID().uuidString.hashValue) {
        self.id = id
    }
}

public struct SocketErrorMessage: Codable {
    public let errorCode: Int
    public let errorMessage: String
    
    public static let retryErrorCode = 666
    public static let retryFailed = SocketErrorMessage(errorCode: SocketErrorMessage.retryErrorCode, errorMessage: "retry failed")
}

// MARK: - Messages
open class ATAReadSocketMessage<T: Decodable>: ATAErrorSocketMessage {
    /// this is the original message sent to the socket that is returned in cas of an error.
    /// it might be needed sometimes
    public var request: T?
    
    enum CodingKeys: String, CodingKey {
        case request
    }
    
    required public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        try super.init(from: decoder)
        do {
            request = error.errorCode ==  0 ? nil : try container.decode(T.self, forKey: .request)
        } catch {
            print(error)
            throw error
        }
    }
}

open class ATAErrorSocketMessage: ATAWriteSocketMessage {
    public var error: SocketErrorMessage
    
    public override init(id: Int,
                         route: SocketRoute) {
        self.error = SocketErrorMessage(errorCode: 0, errorMessage: "")
        super.init(id: id, route: route)
    }
    
    enum CodingKeys: String, CodingKey {
        case status
        case id
    }
    
    required public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        error = try container.decode(SocketErrorMessage.self, forKey: .status)
        try super.init(from: decoder)
    }
}

extension SocketBaseMessage: Hashable, Equatable {
    public static func == (lhs: SocketBaseMessage, rhs: SocketBaseMessage) -> Bool {
        return lhs.hashValue == rhs.hashValue
    }
    public func hash(into hasher: inout Hasher) {
        hasher.combine(id)
    }
}

open class ATAWriteSocketMessage: SocketBaseMessage {
    public let method: SocketRoute
    // use this to check the decoded method and test again the decoded value
    open var checkMethod: SocketRoute? { nil }
    /// if set to true (default), will listen for a read message with the same id during the timeout period.
    /// if no answer is received, it will try again and then fail if no answer is still received
    public var awaitsAnswer: Bool = true
    
    public init(id: Int = UUID().uuidString.hashValue,
                route: SocketRoute) {
        self.method = route
        super.init(id: id)
    }
    
    enum CodingKeys: String, CodingKey {
        case id
        case route = "method"
    }
    
    required public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        //mandatory
        method = try container.decode(String.self, forKey: .route)
        try super.init(from: decoder)
        
        // check that the decoded values matches the expected value if provided
        if let route = checkMethod,
           route != method {
            throw SocketManager.SMError.invalidRoute
        }
    }
    
    open override func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(method, forKey: .route)
        try super.encode(to: encoder)
    }
}
