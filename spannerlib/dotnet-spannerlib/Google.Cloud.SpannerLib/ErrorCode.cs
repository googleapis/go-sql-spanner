namespace SpannerDriver
{

  public enum ErrorCode
  {
    /// <summary>The operation was cancelled.</summary>
    Cancelled = 1,

    /// <summary>
    /// Unknown error. This may happen if an internal error occured or
    /// not enough information was obtained to produce a useful message.
    /// </summary>
    Unknown = 2,

    /// <summary>
    /// A bad SQL statement or other invalid input was sent to Spanner.
    /// </summary>
    InvalidArgument = 3,

    /// <summary>
    /// A timeout has occurred. A <see cref="T:Google.Cloud.Spanner.Data.SpannerTransaction" /> should be
    /// retried if this error is encountered.
    /// </summary>
    DeadlineExceeded = 4,

    /// <summary>
    /// The given resource (Spanner Instance, Database, Table) was not
    /// found.
    /// </summary>
    NotFound = 5,

    /// <summary>
    /// The given resource could be not created because it already exists.
    /// </summary>
    AlreadyExists = 6,

    /// <summary>
    /// The supplied credential from <see cref="P:Google.Cloud.Spanner.Data.SpannerConnectionStringBuilder.CredentialFile" />
    /// or from default application credentials does not have sufficient permission for the request.
    /// </summary>
    PermissionDenied = 7,

    /// <summary>
    /// A resource associated with Spanner has been exhausted. This may occur due to a server-side
    /// failure, or due to the local maximum number of sessions being reached.
    /// </summary>
    ResourceExhausted = 8,

    /// <summary>
    /// Operation was rejected because the system is not in a state required for the
    /// operation's execution.
    /// </summary>
    FailedPrecondition = 9,

    /// <summary>
    /// The operation was aborted due to transient issue such as competing transaction
    /// resources. A <see cref="T:Google.Cloud.Spanner.Data.SpannerTransaction" /> should be retried if this error
    /// is encountered.
    /// </summary>
    Aborted = 10, // 0x0000000A

    /// <summary>The operation attempted to read past the valid range.</summary>
    OutOfRange = 11, // 0x0000000B

    /// <summary>
    /// Operation is not implemented or not supported/enabled in this service.
    /// </summary>
    Unimplemented = 12, // 0x0000000C

    /// <summary>Internal error.</summary>
    Internal = 13, // 0x0000000D

    /// <summary>
    /// The service is currently unavailable. This is a most likely a transient condition
    /// and may be corrected by retrying. A <see cref="T:Google.Cloud.Spanner.Data.SpannerTransaction" /> should be
    /// retried if this error is encountered.
    /// </summary>
    Unavailable = 14, // 0x0000000E

    /// <summary>Unrecoverable data loss or corruption.</summary>
    DataLoss = 15, // 0x0000000F

    /// <summary>
    /// There is no supplied credential either through default application credentials or
    /// directly through <see cref="P:Google.Cloud.Spanner.Data.SpannerConnectionStringBuilder.CredentialFile" />
    /// </summary>
    Unauthenticated = 16, // 0x00000010
  }
}