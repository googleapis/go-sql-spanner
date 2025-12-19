// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Text;

namespace Google.Cloud.Spanner.DataProvider;

public class SpannerConnectionStringBuilder : DbConnectionStringBuilder
{
	/// <summary>
	/// The fully qualified name of the Spanner database to connect to.
	/// Example: projects/my-project/instances/my-instance/databases/my-database
	/// </summary>
	[Category("Connection")]
	[Description("The fully qualified name of the database to use. This property takes precedence over any Project, Instance, or Database that has been set in the connection string.")]
	[DisplayName("Data Source")]
	public string DataSource
	{
		get => SpannerConnectionStringOption.DataSource.GetValue(this);
		set => SpannerConnectionStringOption.DataSource.SetValue(this, value);
	}
	
	/// <summary>
	/// The name of the Spanner instance to connect to.
	/// </summary>
	[Category("Connection")]
	[Description("The name of the Google Cloud project to use.")]
	[DisplayName("Project")]
	public string Project
	{
		get => SpannerConnectionStringOption.Project.GetValue(this);
		set => SpannerConnectionStringOption.Project.SetValue(this, value);
	}

	/// <summary>
	/// The name of the Spanner instance to connect to.
	/// </summary>
	[Category("Connection")]
	[Description("The name of the Spanner instance to use.")]
	[DisplayName("Instance")]
	public string Instance
	{
		get => SpannerConnectionStringOption.Instance.GetValue(this);
		set => SpannerConnectionStringOption.Instance.SetValue(this, value);
	}

	/// <summary>
	/// The name of the Spanner database to connect to.
	/// </summary>
	[Category("Connection")]
	[Description("The name of the database to use")]
	[DisplayName("Database")]
	public string Database
	{
		get => SpannerConnectionStringOption.Database.GetValue(this);
		set => SpannerConnectionStringOption.Database.SetValue(this, value);
	}
	
    /// <summary>
    /// The hostname or IP address of the Spanner server to connect to.
    /// </summary>
    [Category("Connection")]
    [Description("The hostname or IP address of the Spanner server to connect to.")]
    [DefaultValue("")]
    [DisplayName("Host")]
    public string Host
    {
	    get => SpannerConnectionStringOption.Host.GetValue(this);
	    set => SpannerConnectionStringOption.Host.SetValue(this, value);
    }

    /// <summary>
    /// The TCP port of the Spanner server to connect to.
    /// </summary>
    [Category("Connection")]
    [DefaultValue(443u)]
    [Description("The TCP port of the Spanner server to connect to.")]
    [DisplayName("Port")]
    public uint Port
    {
	    get => SpannerConnectionStringOption.Port.GetValue(this);
	    set => SpannerConnectionStringOption.Port.SetValue(this, value);
    }

    /// <summary>
    /// Whether to automatically try to connect to the Spanner Emulator, and automatically create the Spanner
    /// instance and database on the Emulator if these do not already exist. Setting this option to true will
    /// instruct the driver to:
    /// 1. Try to connect to localhost:9010 (unless a different Host/Port has been set in the connection string)
    /// 2. Use plain text communication instead of SSL.
    /// 3. Create the Spanner Instance and Database on the Emulator if these do not already exist.
    /// </summary>
    [Category("Connection")]
    [DefaultValue(false)]
    [Description("Whether to automatically try to connect to the Spanner Emulator and create the Instance and Database on the Emulator.")]
    [DisplayName("AutoConfigEmulator")]
    public bool AutoConfigEmulator
    {
	    get => SpannerConnectionStringOption.AutoConfigEmulator.GetValue(this);
	    set => SpannerConnectionStringOption.AutoConfigEmulator.SetValue(this, value);
    }

    /// <summary>
    /// Whether to use plain text communication with the server. The default is SSL.
    /// </summary>
    [Category("Connection")]
    [DefaultValue(false)]
    [Description("Whether to use plain text or SSL (default).")]
    [DisplayName("UsePlainText")]
    public bool UsePlainText
    {
	    get => SpannerConnectionStringOption.UsePlainText.GetValue(this);
	    set => SpannerConnectionStringOption.UsePlainText.SetValue(this, value);
    }

    /// <summary>
    /// The time in milliseconds to wait for a connection before terminating the attempt and generating an error.
    /// The default value is 15000 (15 seconds).
    /// </summary>
    [Category("Timeout")]
    [Description("The time in milliseconds to wait for a connection before terminating the attempt and generating an error.")]
    [DefaultValue(15000u)]
    [DisplayName("Connection Timeout")]
    public uint ConnectionTimeout
    {
	    get => SpannerConnectionStringOption.ConnectionTimeout.GetValue(this);
	    set => SpannerConnectionStringOption.ConnectionTimeout.SetValue(this, value);
    }

    /// <summary>
    /// The time in milliseconds to wait for a command before terminating the attempt and generating an error.
    /// The default value is 0, which means that the command should use the default timeout set by Spanner.
    /// </summary>
    [Category("Timeout")]
    [Description("The time in milliseconds to wait for a command before terminating the attempt and generating an error.")]
    [DefaultValue(0u)]
    [DisplayName("Command Timeout")]
    public uint CommandTimeout
    {
	    get => SpannerConnectionStringOption.CommandTimeout.GetValue(this);
	    set => SpannerConnectionStringOption.CommandTimeout.SetValue(this, value);
    }

    /// <summary>
    /// The maximum time in milliseconds that a read/write transaction may take to execute.
    /// The default value is 0, which means that there is no transaction timeout.
    /// </summary>
    [Category("Timeout")]
    [Description("The maximum time in milliseconds that a read/write transaction may take to execute.")]
    [DefaultValue(0u)]
    [DisplayName("Transaction Timeout")]
    public uint TransactionTimeout
    {
	    get => SpannerConnectionStringOption.TransactionTimeout.GetValue(this);
	    set => SpannerConnectionStringOption.TransactionTimeout.SetValue(this, value);
    }
	
    /// <summary>
    /// The default isolation level that should be used for transactions on connections created from this connection
    /// string.
    /// </summary>
    [Category("Transaction")]
    [Description("The default isolation level to use for transactions on this connection.")]
    [DefaultValue(IsolationLevel.Unspecified)]
    [DisplayName("DefaultIsolationLevel")]
    public IsolationLevel DefaultIsolationLevel
    {
	    get => SpannerConnectionStringOption.DefaultIsolationLevel.GetValue(this);
	    set => SpannerConnectionStringOption.DefaultIsolationLevel.SetValue(this, value);
    }
	
    /// <summary>
    /// The search_path that should be used by the connection.
    /// </summary>
    [Category("Options")]
    [Description("The search path for this connection.")]
    [DefaultValue("")]
    [DisplayName("SearchPath")]
    public string SearchPath
    {
	    get => SpannerConnectionStringOption.SearchPath.GetValue(this);
	    set => SpannerConnectionStringOption.SearchPath.SetValue(this, value);
    }

    /// <summary>
    /// Any other options that should be set for the connection in the format key1=value1;key2=value2;...
    /// </summary>
    [Category("Options")]
    [Description("Any additional options to set for the connection.")]
    [DefaultValue("")]
    [DisplayName("Options")]
    public string Options
    {
	    get => SpannerConnectionStringOption.Options.GetValue(this);
	    set => SpannerConnectionStringOption.Options.SetValue(this, value);
    }

	/// <summary>
	/// Returns an <see cref="ICollection"/> that contains the keys in the <see cref="SpannerConnectionStringBuilder"/>.
	/// </summary>
	public override ICollection Keys => base.Keys.Cast<string>().OrderBy(static x => SpannerConnectionStringOption.OptionNames.IndexOf(x)).ToList();

	/// <summary>
	/// Whether this <see cref="SpannerConnectionStringBuilder"/> contains a set option with the specified name.
	/// </summary>
	/// <param name="keyword">The option name.</param>
	/// <returns><c>true</c> if an option with that name is set; otherwise, <c>false</c>.</returns>
	public override bool ContainsKey(string keyword) =>
		SpannerConnectionStringOption.TryGetOptionForKey(keyword) is { } option && base.ContainsKey(option.Key);

	/// <summary>
	/// Removes the option with the specified name.
	/// </summary>
	/// <param name="keyword">The option name.</param>
	public override bool Remove(string keyword) =>
		SpannerConnectionStringOption.TryGetOptionForKey(keyword) is { } option && base.Remove(option.Key);

	/// <summary>
	/// Retrieves an option value by name.
	/// </summary>
	/// <param name="key">The option name.</param>
	/// <returns>That option's value, if set.</returns>
	[AllowNull]
	public override object this[string key]
	{
		get
		{
			var option = SpannerConnectionStringOption.TryGetOptionForKey(key);
			return option == null ? base[key] : option.GetObject(this);
		}
		set
		{
			var option = SpannerConnectionStringOption.TryGetOptionForKey(key);
			if (option == null)
			{
				base[key] = value;
			}
			else
			{
				if (value is null)
				{
					base[option.Key] = null;
				}
				else
				{
					option.SetObject(this, value);
				}
			}
		}
	}

	public SpannerConnectionStringBuilder()
	{
	}
	
	public SpannerConnectionStringBuilder(string connectionString)
	{
		ConnectionString = connectionString;
	}

	internal void DoSetValue(string key, object? value) => base[key] = value;
	
	internal SpannerConnectionStringBuilder Clone() => new(ConnectionString);

	internal void CheckValid()
	{
		if (string.IsNullOrEmpty(ConnectionString))
		{
			throw new ArgumentException("Empty connection string");
		}
		if (string.IsNullOrEmpty(DataSource))
		{
			if (string.IsNullOrEmpty(Project) || string.IsNullOrEmpty(Instance) || string.IsNullOrEmpty(Database))
			{
				throw new ArgumentException("The connection string must either contain a Data Source or a Project, Instance, and Database name");
			}
		}
	}

	internal string SpannerLibConnectionString
	{
		get
		{
			CheckValid();
			var builder = new StringBuilder();
			if (Host != "")
			{
				builder.Append(Host);
				if (Port != 443)
				{
					builder.Append(":");
					builder.Append(Port);
				}
				builder.Append('/');
			}
			if (DataSource != "")
			{
				builder.Append(DataSource);
			}
			else if (Project != "" && Instance != "" && Database != "")
			{
				builder.Append("projects/").Append(Project);
				builder.Append("/instances/").Append(Instance);
				builder.Append("/databases/").Append(Database);
			}
			else
			{
				throw new ArgumentException("Invalid connection string. Either Data Source or Project, Instance, and Database must be specified.");
			}
			foreach (var key in Keys.Cast<string>())
			{
				if (SpannerConnectionStringOption.SOptions.ContainsKey(key))
				{
					var option = SpannerConnectionStringOption.SOptions[key];
					if (option.SpannerLibKey != "")
					{
						builder.Append(';').Append(option.SpannerLibKey).Append('=').Append(this[key]);
					}
					else if (key == "Options")
					{
						builder.Append(';').Append(this[key]);
					}
				}
				else
				{
					builder.Append(';').Append(key).Append('=').Append(this[key]);
				}
			}
			return builder.ToString();
		}
	}

}

internal abstract class SpannerConnectionStringOption
{
	public static List<string> OptionNames { get; } = [];

	// Connection Options
	public static readonly SpannerConnectionStringReferenceOption<string> DataSource;
	public static readonly SpannerConnectionStringReferenceOption<string> Host;
	public static readonly SpannerConnectionStringValueOption<uint> Port;
	public static readonly SpannerConnectionStringReferenceOption<string> Project;
	public static readonly SpannerConnectionStringReferenceOption<string> Instance;
	public static readonly SpannerConnectionStringReferenceOption<string> Database;
	
	// Timeout Options
	public static readonly SpannerConnectionStringValueOption<uint> ConnectionTimeout;
	public static readonly SpannerConnectionStringValueOption<uint> CommandTimeout;
	public static readonly SpannerConnectionStringValueOption<uint> TransactionTimeout;

	// Emulator Options
	public static readonly SpannerConnectionStringValueOption<bool> AutoConfigEmulator;
	
	// SSL/TLS Options
	public static readonly SpannerConnectionStringValueOption<bool> UsePlainText;
	
	// Transaction Options
	public static readonly SpannerConnectionStringValueOption<IsolationLevel> DefaultIsolationLevel;
	
	// Other options
	public static readonly SpannerConnectionStringReferenceOption<string> SearchPath;
	public static readonly SpannerConnectionStringReferenceOption<string> Options;

	public static SpannerConnectionStringOption? TryGetOptionForKey(string key) => SOptions.GetValueOrDefault(key);

	public static SpannerConnectionStringOption GetOptionForKey(string key) =>
		TryGetOptionForKey(key) ?? throw new ArgumentException($"Option '{key}' not supported.");

	public string Key => _keys[0];
	public IReadOnlyList<string> Keys => _keys;
	
	internal string SpannerLibKey { get; }

	public abstract object GetObject(SpannerConnectionStringBuilder builder);
	public abstract void SetObject(SpannerConnectionStringBuilder builder, object value);

	protected SpannerConnectionStringOption(IReadOnlyList<string> keys) : this(keys, keys[0])
	{
	}

	protected SpannerConnectionStringOption(IReadOnlyList<string> keys, string spannerLibKey)
	{
		_keys = keys;
		SpannerLibKey = spannerLibKey;
	}

	private static void AddOption(Dictionary<string, SpannerConnectionStringOption> options, SpannerConnectionStringOption option)
	{
		foreach (var key in option._keys)
		{
			options.Add(key, option);
		}
		OptionNames.Add(option._keys[0]);
	}

	static SpannerConnectionStringOption()
	{
		var options = new Dictionary<string, SpannerConnectionStringOption>(StringComparer.OrdinalIgnoreCase);

		// Base Options
		AddOption(options, DataSource = new(
			keys: ["Data Source", "DataSource"],
			spannerLibKey: "",
			defaultValue: ""));
		
		AddOption(options, Host = new(
			keys: ["Host", "Server"],
			spannerLibKey: "",
			defaultValue: ""));

		AddOption(options, Port = new(
			keys: ["Port"],
			spannerLibKey: "",
			defaultValue: 443u));

		AddOption(options, Project = new(
			keys: ["Project"],
			spannerLibKey: "",
			defaultValue: ""));

		AddOption(options, Instance = new(
			keys: ["Instance"],
			spannerLibKey: "",
			defaultValue: ""));

		AddOption(options, Database = new(
			keys: ["Database", "Initial Catalog"],
			spannerLibKey: "",
			defaultValue: ""));

		// Timeout Options
		AddOption(options, ConnectionTimeout = new(
			keys: ["Connection Timeout", "ConnectionTimeout", "Connect Timeout", "connect_timeout"],
			spannerLibKey: "connect_timeout",
			defaultValue: 15000u));

		AddOption(options, CommandTimeout = new(
			keys: ["Command Timeout", "CommandTimeout", "command_timeout", "statement_timeout"],
			spannerLibKey: "statement_timeout",
			defaultValue: 0u));

		AddOption(options, TransactionTimeout = new(
			keys: ["Transaction Timeout", "TransactionTimeout", "transaction_timeout"],
			spannerLibKey: "transaction_timeout",
			defaultValue: 0u));

		// Emulator Options
		AddOption(options, AutoConfigEmulator = new(
			keys: ["AutoConfigEmulator", "Auto Config Emulator", "UseEmulator", "Use Emulator", "auto_config_emulator"],
			defaultValue: false));

		// SSL/TLS Options
		AddOption(options, UsePlainText = new(
			keys: ["UsePlainText", "Use plain text", "Plain text", "use_plain_text"],
			defaultValue: false));
		
		// Transaction Options
		AddOption(options, DefaultIsolationLevel = new(
			keys: ["DefaultIsolationLevel", "default_isolation_level"],
			defaultValue: IsolationLevel.Unspecified));
		
		// Other options
		AddOption(options, SearchPath = new(
			keys: ["SearchPath", "search_path"],
			spannerLibKey: "search_path",
			defaultValue: ""));
		
		// Other options
		AddOption(options, Options = new(
			keys: ["Options"],
			spannerLibKey: "",
			defaultValue: ""));

		SOptions = options.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);
	}

	internal static readonly FrozenDictionary<string, SpannerConnectionStringOption> SOptions;

	private readonly IReadOnlyList<string> _keys;
}

internal sealed class SpannerConnectionStringValueOption<T> : SpannerConnectionStringOption
	where T : struct
{
	public SpannerConnectionStringValueOption(IReadOnlyList<string> keys, T defaultValue, Func<T, T>? coerce = null)
		: this(keys, keys[0], defaultValue, coerce)
	{
	}
	
	public SpannerConnectionStringValueOption(IReadOnlyList<string> keys, string spannerLibKey, T defaultValue, Func<T, T>? coerce = null)
		: base(keys, spannerLibKey)
	{
		DefaultValue = defaultValue;
		_coerce = coerce;
	}
	
	public T DefaultValue { get; }

	public T GetValue(SpannerConnectionStringBuilder builder) =>
		builder.TryGetValue(Key, out var objectValue) ? ChangeType(objectValue) : DefaultValue;

	public void SetValue(SpannerConnectionStringBuilder builder, T value) =>
		builder.DoSetValue(Key, _coerce is null ? value : _coerce(value));

	public override object GetObject(SpannerConnectionStringBuilder builder) => GetValue(builder);

	public override void SetObject(SpannerConnectionStringBuilder builder, object value) => SetValue(builder, ChangeType(value));

	private T ChangeType(object objectValue)
	{
		if (typeof(T) == typeof(bool) && objectValue is string booleanString)
		{
			if (string.Equals(booleanString, "yes", StringComparison.OrdinalIgnoreCase))
			{
				return (T)(object)true;
			}
			if (string.Equals(booleanString, "on", StringComparison.OrdinalIgnoreCase))
			{
				return (T)(object)true;
			}
			if (string.Equals(booleanString, "no", StringComparison.OrdinalIgnoreCase))
			{
				return (T)(object)false;
			}
			if (string.Equals(booleanString, "off", StringComparison.OrdinalIgnoreCase))
			{
				return (T)(object)false;
			}
		}

		if (typeof(T).IsEnum && objectValue is string enumString)
		{
			enumString = enumString.Trim().Replace("_", "").Replace(" ", "");
			return (T)Enum.Parse(typeof(T), enumString, ignoreCase: true);
		}

		try
		{
			return (T) Convert.ChangeType(objectValue, typeof(T), CultureInfo.InvariantCulture);
		}
		catch (Exception ex)
		{
			var exceptionMessage = string.Create(CultureInfo.InvariantCulture, $"Invalid value '{objectValue}' for '{Key}' connection string option.");
			throw new ArgumentException(exceptionMessage, ex);
		}
	}

	private readonly Func<T, T>? _coerce;
}

internal sealed class SpannerConnectionStringReferenceOption<T> : SpannerConnectionStringOption
	where T : class
{
	public SpannerConnectionStringReferenceOption(IReadOnlyList<string> keys, string spannerLibKey, T defaultValue, Func<T?, T>? coerce = null)
		: base(keys, spannerLibKey)
	{
		DefaultValue = defaultValue;
		_coerce = coerce;
	}

	public T DefaultValue { get; }

	public T GetValue(SpannerConnectionStringBuilder builder) =>
		builder.TryGetValue(Key, out var objectValue) ? ChangeType(objectValue) : DefaultValue;

	public void SetValue(SpannerConnectionStringBuilder builder, T? value) =>
		builder.DoSetValue(Key, _coerce is null ? value : _coerce(value));

	public override object GetObject(SpannerConnectionStringBuilder builder) => GetValue(builder);

	public override void SetObject(SpannerConnectionStringBuilder builder, object value) => SetValue(builder, ChangeType(value));

	private static T ChangeType(object objectValue) =>
		(T) Convert.ChangeType(objectValue, typeof(T), CultureInfo.InvariantCulture);

	private readonly Func<T?, T>? _coerce;
}
