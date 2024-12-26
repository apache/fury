namespace Fury.Testing;

public class BuiltInsTest
{
    [Fact]
    public void BuiltInTypeInfos_IdShouldMatchItsIndex()
    {
        var typeIds = BuiltIns.TypeIds;
        for (var i = 0; i < typeIds.Count; i++)
        {
            Assert.Equal(i, typeIds[i].Value);
        }
    }
}
