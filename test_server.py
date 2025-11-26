from mcp.server.fastmcp import FastMCP
import os

# Create an MCP server
mcp = FastMCP("AI Sticky Notes", json_response=True)

NOTES_FILE = os.path.join(os.path.dirname(__file__), "notes.txt")

def ensure_file():
    if not os.path.exists(NOTES_FILE):
        with open(NOTES_FILE, "w") as f:
            f.write("")

@mcp.tool()
def add_note(note: str) -> str:
    """
    Append a new note to the notes file.

    Args:
        message (str): The message to add to the notes file.

    Returns:
        str: A message indicating the note was saved.
    """
    ensure_file()
    with open(NOTES_FILE, "a") as f:
        f.write(note + "\n")
    return "Note saved"

@mcp.tool()
def read_notes() -> str:
    """
    Read the notes file and return the contents.

    Returns:
        str: The contents of the notes file.
    """
    ensure_file()
    with open(NOTES_FILE, "r") as f:
        content = f.read().strip()
    return content or "No notes found"

@mcp.resource("notes://latest")
def get_latest_note() -> str:
    """
    Get the latest note from the notes file.

    Returns:
        str: The latest note from the notes file. If no notes exist, a message will be shown indicating that.
    """
    ensure_file()
    with open(NOTES_FILE, "r") as f:
        lines = f.readlines()
    return lines[-1].strip() if lines else "No notes found"

@mcp.prompt()
def note_summary_prompt() -> str:
    """
    Generate a prompt asking the AI to summarize all current notes.

    Returns:
        str: A prompt string that includes all notes and asks for a summary.
            If no notes exist, a message will be shown indicating that.
    """
    ensure_file()
    with open(NOTES_FILE, "r") as f:
        content = f.read().strip()
    if not content:
        return "No notes found"
    return f"Summarize the following notes: {content}"


# Run with stdio transport for MCP protocol
if __name__ == "__main__":
    mcp.run()
