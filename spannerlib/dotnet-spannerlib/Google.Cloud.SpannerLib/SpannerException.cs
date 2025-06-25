using System.Data;
using SpannerDriver;

namespace Google.Cloud.SpannerLib
{

    public class SpannerException : DataException
    {
        /// <summary>
        /// An error code that indicates the general class of problem.
        /// </summary>
        public ErrorCode ErrorCode { get; }

        internal SpannerException(int code, string message) : this((ErrorCode)code, message)
        {
        }

        internal SpannerException(ErrorCode code, string message) : base(message)
        {
            ErrorCode = code;
        }
    }
}