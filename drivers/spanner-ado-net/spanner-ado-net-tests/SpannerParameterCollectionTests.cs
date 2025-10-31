using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace Google.Cloud.Spanner.DataProvider.Tests;

public class SpannerParameterCollectionTests : AbstractMockServerTests
{
    [Test]
    public void CanOnlyAddSpannerParameterOrValidValue()
    {
        using var command = new SpannerCommand();
        
        Assert.DoesNotThrow(() => command.Parameters.Add("hello"));
        
        Assert.That(() => command.Parameters.Add(new SomeOtherDbParameter()), Throws.Exception.TypeOf<ArgumentException>());
        Assert.That(() => command.Parameters.Add(null!), Throws.Exception.TypeOf<ArgumentNullException>());
    }
    
    [Test]
    public void Clear()
    {
        var p = new SpannerParameter();
        var c1 = new SpannerCommand();
        var c2 = new SpannerCommand();
        c1.Parameters.Add(p);
        Assert.That(c1.Parameters.Count, Is.EqualTo(1));
        Assert.That(c2.Parameters.Count, Is.EqualTo(0));
        c1.Parameters.Clear();
        Assert.That(c1.Parameters.Count, Is.EqualTo(0));
        c2.Parameters.Add(p);
        Assert.That(c1.Parameters.Count, Is.EqualTo(0));
        Assert.That(c2.Parameters.Count, Is.EqualTo(1));
    }
    
    [Test]
    public void ParameterRename()
    {
        using var command = new SpannerCommand();
        for (var i = 0; i < 10; i++)
        {
            command.AddParameter($"p{i + 1:00}", $"String parameter value {i + 1}");
        }
        Assert.That(command.Parameters["p03"].ParameterName, Is.EqualTo("p03"));

        // Rename a parameter.
        command.Parameters["p03"].ParameterName = "a_new_name";
        Assert.That(command.Parameters.IndexOf("a_new_name"), Is.GreaterThanOrEqualTo(0));
    }
    
    [Test]
    public void UnnamedParameterRename()
    {
        using var command = new SpannerCommand();

        for (var i = 0; i < 3; i++)
        {
            for (var j = 0; j < 10; j++)
            {
                // Create and add an unnamed parameter before renaming it
                var parameter = command.CreateParameter();
                command.Parameters.Add(parameter);
                parameter.ParameterName = $"{j}";
            }
            Assert.That(command.Parameters["3"].ParameterName, Is.EqualTo("3"));
            command.Parameters.Clear();
        }
    }
    
    [Test]
    public void RemoveDuplicateParameter()
    {
        using var command = new SpannerCommand();
        var count = 10;
        for (var i = 0; i < count; i++)
        {
            command.AddParameter($"p{i + 1:00}", $"String parameter value {i + 1}");
        }

        Assert.That(command.Parameters["p02"].ParameterName, Is.EqualTo("p02"));
        // Add uppercased version of the same parameter.
        command.AddParameter("P02", "String parameter value 2");
        // Remove the original parameter by its name.
        command.Parameters.Remove(command.Parameters["p02"]);

        // Test whether we can still find the last added parameter, and if its index is correctly shifted in the lookup.
        Assert.That(command.Parameters.IndexOf("p02"), Is.EqualTo(count - 1));
        Assert.That(command.Parameters.IndexOf("P02"), Is.EqualTo(count - 1));
        // And finally test whether other parameters were also correctly shifted.
        Assert.That(command.Parameters.IndexOf("p03"), Is.EqualTo(1));
        Assert.That(command.Parameters.IndexOf("p03") == 1);
    }

    [Test]
    public void RemoveParameter()
    {
        using var command = new SpannerCommand();
        var count = 10;
        for (var i = 0; i < count; i++)
        {
            command.AddParameter($"p{i + 1:00}", $"String parameter value {i + 1}");
        }

        // Remove the parameter by its name
        command.Parameters.Remove(command.Parameters["p02"]);

        // Make sure we cannot find it, also not case insensitively.
        Assert.That(command.Parameters.IndexOf("p02"), Is.EqualTo(-1));
        Assert.That(command.Parameters.IndexOf("P02"), Is.EqualTo(-1));
    }

    [Test]
    public void RemoveCaseDifferingParameter()
    {
        var count = 10;
        // Add two parameters that only differ in casing.
        using var command = new SpannerCommand();
        command.AddParameter("PP0", 1);
        command.AddParameter("Pp0", 1);
        for (var i = 0; i < count - 2; i++)
        {
            command.AddParameter($"pp{i}", i);
        }

        // Removing Pp0.
        command.Parameters.RemoveAt(1);

        // Matching on parameter name always first prefers case-sensitive matching, so we match entry 1 ('pp0').
        Assert.That(command.Parameters.IndexOf("pp0"), Is.EqualTo(1));
        // Exact match to PP0.
        Assert.That(command.Parameters.IndexOf("PP0"), Is.EqualTo(0));
        // Case-insensitive match to PP0.
        Assert.That(command.Parameters.IndexOf("Pp0"), Is.EqualTo(0));
    }
    
    [Test]
    public void CorrectIndexReturnedForDuplicateParameterName()
    {
        const int count = 10;
        using var command = new SpannerCommand();
        for (var i = 0; i < count; i++)
        {
            command.AddParameter($"parameter{i + 1:00}", $"String parameter value {i + 1}");
        }
        Assert.That(command.Parameters["parameter02"].ParameterName, Is.EqualTo("parameter02"));

        // Add an upper-case version of one of the parameters.
        command.AddParameter("Parameter02", "String parameter value 2");

        // Insert another case-insensitive before the original.
        command.Parameters.Insert(0, new SpannerParameter { ParameterName = "ParameteR02", Value = "String parameter value 2" });

        // Try to find the exact index.
        Assert.That(command.Parameters.IndexOf("parameter02"), Is.EqualTo(2));
        Assert.That(command.Parameters.IndexOf("Parameter02"), Is.EqualTo(command.Parameters.Count - 1));
        Assert.That(command.Parameters.IndexOf("ParameteR02"), Is.EqualTo(0));
        // This name does not exist so we expect the first case-insensitive match to be returned.
        Assert.That(command.Parameters.IndexOf("ParaMeteR02"), Is.EqualTo(0));

        // And finally test whether other parameters were also correctly shifted.
        Assert.That(command.Parameters.IndexOf("parameter03"), Is.EqualTo(3));
    }

    [Test]
    public void FindsCaseInsensitiveLookups()
    {
        const int count = 10;
        using var command = new SpannerCommand();
        var parameters = command.Parameters;
        for (var i = 0; i < count; i++)
        {
            parameters.Add(new SpannerParameter{ ParameterName = $"p{i}", Value = i });
        }
        Assert.That(command.Parameters.IndexOf("P1"), Is.EqualTo(1));
    }

    [Test]
    public void FindsCaseSensitiveLookups()
    {
        const int count = 10;
        using var command = new SpannerCommand();
        var parameters = command.Parameters;
        for (var i = 0; i < count; i++)
        {
            parameters.Add(new SpannerParameter{ ParameterName = $"p{i}", Value = i});
        }
        Assert.That(command.Parameters.IndexOf("p1"), Is.EqualTo(1));
    }

    [Test]
    public void ThrowsOnIndexerMismatch()
    {
        const int count = 10;
        using var command = new SpannerCommand();
        var parameters = command.Parameters;
        for (var i = 0; i < count; i++)
        {
            parameters.Add(new SpannerParameter{ ParameterName = $"p{i}", Value = i});
        }

        Assert.DoesNotThrow(() =>
        {
            command.Parameters["p1"] = new SpannerParameter("p1", 1);
            command.Parameters["p1"] = new SpannerParameter("P1", 1);
        });

        Assert.Throws<ArgumentException>(() =>
        {
            command.Parameters["p1"] = new SpannerParameter("p2", 1);
        });
    }

    [Test]
    public void PositionalParameterLookupReturnsFirstMatch()
    {
        const int count = 10;
        using var command = new SpannerCommand();
        var parameters = command.Parameters;
        for (var i = 0; i < count; i++)
        {
            parameters.Add(new SpannerParameter("", i));
        }
        Assert.That(command.Parameters.IndexOf(""), Is.EqualTo(0));
    }

    [Test]
    public void MultiplePositionsSameInstanceIsAllowed()
    {
        using var cmd = new SpannerCommand();
        cmd.CommandText = "SELECT $1, $2";
        var p = new SpannerParameter("", "Hello world");
        cmd.Parameters.Add(p);
        Assert.DoesNotThrow(() => cmd.Parameters.Add(p));
    }

    [Test]
    public void IndexOfFallsBackToFirstInsensitiveMatch([Values] bool manyParams)
    {
        const int count = 10;
        using var command = new SpannerCommand();
        var parameters = command.Parameters;

        parameters.Add(new SpannerParameter("foo", 8));
        parameters.Add(new SpannerParameter("bar", 8));
        parameters.Add(new SpannerParameter("BAR", 8));

        if (manyParams)
        {
            for (var i = 0; i < count; i++)
            {
                parameters.Add(new SpannerParameter($"p{i}", i));
            }
        }
        Assert.That(parameters.IndexOf("Bar"), Is.EqualTo(1));
    }

    [Test]
    public void IndexOfPrefersCaseSensitiveMatch([Values] bool manyParams)
    {
        const int count = 10;
        using var command = new SpannerCommand();
        var parameters = command.Parameters;

        parameters.Add(new SpannerParameter("FOO", 8));
        parameters.Add(new SpannerParameter("foo", 8));

        if (manyParams)
        {
            for (var i = 0; i < count; i++)
            {
                parameters.Add(new SpannerParameter($"p{i}", i));
            }
        }
        Assert.That(parameters.IndexOf("foo"), Is.EqualTo(1));
    }

    [Test]
    public void CloningSucceeds()
    {
        const int count = 10;
        var command = new SpannerCommand();
        for (var i = 0; i < count; i++)
        {
            command.Parameters.Add(new SpannerParameter());
        }
        Assert.DoesNotThrow(() => command.Clone());
    }
    
    [Test]
    public void CleanName()
    {
        var param = new SpannerParameter();
        var command = new SpannerCommand();
        command.Parameters.Add(param);

        param.ParameterName = null;

        // These should not throw exceptions
        Assert.That(command.Parameters.IndexOf(param.ParameterName), Is.EqualTo(0));
        Assert.That(param.ParameterName, Is.EqualTo(""));
    }
    
    class SomeOtherDbParameter : DbParameter
    {
        public override void ResetDbType() {}

        public override DbType DbType { get; set; }
        public override ParameterDirection Direction { get; set; }
        public override bool IsNullable { get; set; }
        [AllowNull] public override string ParameterName { get; set; } = "";
        [AllowNull] public override string SourceColumn { get; set; } = "";
        public override object? Value { get; set; }
        public override bool SourceColumnNullMapping { get; set; }
        public override int Size { get; set; }
    }

}