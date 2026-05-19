namespace H.ToolBox.RabbitMQ.Model
{
    /// <summary>
    /// 操作结果结构，用于封装操作的执行状态和可能的错误信息
    /// </summary>
    public struct OperateResult
    {
        private readonly OperateError? _operateError = null;

        /// <summary>
        /// 获取或设置操作是否成功
        /// </summary>
        public bool Succeeded { get; set; }

        /// <summary>
        /// 获取或设置操作过程中发生的异常
        /// </summary>
        public Exception? Exception { get; set; }

        /// <summary>
        /// 初始化操作结果实例
        /// </summary>
        /// <param name="succeeded">操作是否成功</param>
        /// <param name="exception">操作异常（可选）</param>
        /// <param name="error">操作错误详情（可选）</param>
        public OperateResult(bool succeeded, Exception? exception = null, OperateError? error = null)
        {
            Succeeded = succeeded;
            Exception = exception;
            _operateError = error;
        }

        /// <summary>
        /// 表示成功的操作结果
        /// </summary>
        public static OperateResult Success => new(true);

        /// <summary>
        /// 创建失败的操作结果
        /// </summary>
        /// <param name="ex">导致失败的异常</param>
        /// <param name="errors">错误详情（可选）</param>
        /// <returns>失败的操作结果实例</returns>
        public static OperateResult Failed(Exception ex, OperateError? errors = null)
        {
            return new(false, ex, errors);
        }
    }

    /// <summary>
    /// 操作错误信息结构，包含错误代码和描述
    /// </summary>
    public struct OperateError
    {
        /// <summary>
        /// 获取或设置错误代码
        /// </summary>
        public string Code { get; set; }

        /// <summary>
        /// 获取或设置错误描述信息
        /// </summary>
        public string Description { get; set; }
    }
}