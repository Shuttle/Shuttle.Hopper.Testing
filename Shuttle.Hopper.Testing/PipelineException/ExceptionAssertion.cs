namespace Shuttle.Hopper.Testing;

internal class ExceptionAssertion(string name)
{
    public bool HasRun { get; private set; }

    public string Name { get; private set; } = name;

    public void MarkAsRun()
    {
        HasRun = true;
    }
}