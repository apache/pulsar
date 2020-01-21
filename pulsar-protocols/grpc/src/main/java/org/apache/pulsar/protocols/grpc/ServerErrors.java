package org.apache.pulsar.protocols.grpc;

public class ServerErrors {

    public static PulsarApi.ServerError convert(org.apache.pulsar.common.api.proto.PulsarApi.ServerError serverError) {
        switch(serverError) {
            case MetadataError:
                return PulsarApi.ServerError.MetadataError;
            case PersistenceError:
                return PulsarApi.ServerError.PersistenceError;
            case AuthenticationError:
                return PulsarApi.ServerError.AuthenticationError;
            case AuthorizationError:
                return PulsarApi.ServerError.AuthorizationError;
            case ConsumerBusy:
                return PulsarApi.ServerError.ConsumerBusy;
            case ServiceNotReady:
                return PulsarApi.ServerError.ServiceNotReady;
            case ProducerBlockedQuotaExceededError:
                return PulsarApi.ServerError.ProducerBlockedQuotaExceededError;
            case ProducerBlockedQuotaExceededException:
                return PulsarApi.ServerError.ProducerBlockedQuotaExceededException;
            case ChecksumError:
                return PulsarApi.ServerError.ChecksumError;
            case UnsupportedVersionError:
                return PulsarApi.ServerError.UnsupportedVersionError;
            case TopicNotFound:
                return PulsarApi.ServerError.TopicNotFound;
            case SubscriptionNotFound:
                return PulsarApi.ServerError.SubscriptionNotFound;
            case ConsumerNotFound:
                return PulsarApi.ServerError.ConsumerNotFound;
            case TooManyRequests:
                return PulsarApi.ServerError.TooManyRequests;
            case TopicTerminatedError:
                return PulsarApi.ServerError.TopicTerminatedError;
            case ProducerBusy:
                return PulsarApi.ServerError.ProducerBusy;
            case InvalidTopicName:
                return PulsarApi.ServerError.InvalidTopicName;
            case IncompatibleSchema:
                return PulsarApi.ServerError.IncompatibleSchema;
            case ConsumerAssignError:
                return PulsarApi.ServerError.ConsumerAssignError;
            case TransactionCoordinatorNotFound:
                return PulsarApi.ServerError.TransactionCoordinatorNotFound;
            case InvalidTxnStatus:
                return PulsarApi.ServerError.InvalidTxnStatus;
            case UnknownError:
            default:
                return PulsarApi.ServerError.UnknownError;
        }
    }
}
