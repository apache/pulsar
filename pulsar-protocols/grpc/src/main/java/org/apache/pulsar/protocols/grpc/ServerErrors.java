package org.apache.pulsar.protocols.grpc;

import io.grpc.Metadata;
import org.apache.pulsar.protocols.grpc.api.ServerError;

import static org.apache.pulsar.protocols.grpc.Constants.ERROR_CODE_METADATA_KEY;

public class ServerErrors {

    public static ServerError convert(org.apache.pulsar.common.api.proto.PulsarApi.ServerError serverError) {
        switch(serverError) {
            case MetadataError:
                return ServerError.MetadataError;
            case PersistenceError:
                return ServerError.PersistenceError;
            case AuthenticationError:
                return ServerError.AuthenticationError;
            case AuthorizationError:
                return ServerError.AuthorizationError;
            case ConsumerBusy:
                return ServerError.ConsumerBusy;
            case ServiceNotReady:
                return ServerError.ServiceNotReady;
            case ProducerBlockedQuotaExceededError:
                return ServerError.ProducerBlockedQuotaExceededError;
            case ProducerBlockedQuotaExceededException:
                return ServerError.ProducerBlockedQuotaExceededException;
            case ChecksumError:
                return ServerError.ChecksumError;
            case UnsupportedVersionError:
                return ServerError.UnsupportedVersionError;
            case TopicNotFound:
                return ServerError.TopicNotFound;
            case SubscriptionNotFound:
                return ServerError.SubscriptionNotFound;
            case ConsumerNotFound:
                return ServerError.ConsumerNotFound;
            case TooManyRequests:
                return ServerError.TooManyRequests;
            case TopicTerminatedError:
                return ServerError.TopicTerminatedError;
            case ProducerBusy:
                return ServerError.ProducerBusy;
            case InvalidTopicName:
                return ServerError.InvalidTopicName;
            case IncompatibleSchema:
                return ServerError.IncompatibleSchema;
            case ConsumerAssignError:
                return ServerError.ConsumerAssignError;
            case TransactionCoordinatorNotFound:
                return ServerError.TransactionCoordinatorNotFound;
            case InvalidTxnStatus:
                return ServerError.InvalidTxnStatus;
            case UnknownError:
            default:
                return ServerError.UnknownError;
        }
    }

    public static Metadata newErrorMetadata(ServerError error) {
        Metadata metadata  = new Metadata();
        metadata.put(ERROR_CODE_METADATA_KEY, String.valueOf(error.getNumber()));
        return metadata;
    }

    public static Metadata newErrorMetadata(org.apache.pulsar.common.api.proto.PulsarApi.ServerError error) {
        return newErrorMetadata(convert(error));
    }
}
