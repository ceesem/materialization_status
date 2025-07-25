# /// script
# dependencies = [
#   "rich",
#   "caveclient",
#   "tqdm",
# ]
# ///

from caveclient import CAVEclient
import datetime
from rich import print, table, console, text
import tqdm
from typing import Dict, Any, List, Tuple, Optional

SERVERS_TO_CHECK = [
    "https://global.daf-apis.com",
    "https://globalv1.em.brain.allentech.org",
]

# If you want to check specific datasets instead of all available ones,
# you can specify them here. Otherwise, it will check all datastacks on the servers
DATASTACKS_TO_CHECK = []

# Configuration for table columns and their formatting
TABLE_COLUMNS = [
    {"name": "Datastack", "justify": "left"},
    {"name": "Server", "justify": "left"},
    {"name": "Server Version", "justify": "left"},
    {"name": "Expected Version", "justify": "left"},
    {"name": "Latest Version", "justify": "left"},
    {"name": "Latest Timestamp", "justify": "left"},
    {"name": "Days Old", "justify": "left"},
    {"name": "Status", "justify": "left"},
]

# Styling configuration based on success status
STYLE_CONFIG = {
    "success": {"color": "green", "style": ""},
    "failure": {"color": "red", "style": "bold"},
}


def check_materialization(
    datastack: str,
) -> Tuple[Optional[int], Optional[int], Optional[datetime.datetime], str]:
    """Check materialization status for a given datastack."""
    client = CAVEclient(datastack)
    try:
        latest_version = max(client.materialize.get_versions())
        expected_latest_version = max(client.materialize.get_versions(expired=True))
        latest_timestamp = client.materialize.get_timestamp(latest_version)
        server_version = str(client.materialize.server_version)
    except Exception:
        latest_version = None
        expected_latest_version = None
        latest_timestamp = None
        server_version = None
    return (
        latest_version,
        expected_latest_version,
        latest_timestamp,
        client.local_server,
        server_version,
    )


def get_style_config(success: bool) -> Dict[str, str]:
    """Get style configuration based on success status."""
    return STYLE_CONFIG["success"] if success else STYLE_CONFIG["failure"]


def create_styled_text(value: str, style_config: Dict[str, str]) -> text.Text:
    """Create a styled Text object with the given value and style."""
    styled_text = text.Text(value)
    style_str = f"{style_config['style']} {style_config['color']}".strip()
    if style_str:
        styled_text.stylize(style_str)
    return styled_text


def format_row_data(
    datastack: str,
    server: str,
    server_version: str,
    latest_version: int,
    expected_latest_version: int,
    latest_timestamp: datetime.datetime,
    current_day: datetime.date,
    success: bool,
) -> List[text.Text]:
    """Format all row data with appropriate styling."""
    style_config = get_style_config(success)

    # Calculate days old
    days_old = (current_day - latest_timestamp.date()).days

    # Format timestamp
    formatted_timestamp = latest_timestamp.date().strftime("%Y-%m-%d")

    # Status message
    status_message = "Success" if success else "Failed"

    # Create styled text for each column
    row_data = [
        create_styled_text(datastack, style_config),
        create_styled_text(server, style_config),
        create_styled_text(server_version, style_config),
        create_styled_text(str(expected_latest_version), style_config),
        create_styled_text(str(latest_version), style_config),
        create_styled_text(formatted_timestamp, style_config),
        create_styled_text(str(days_old), style_config),
        create_styled_text(status_message, style_config),
    ]

    return row_data


def create_result_table() -> table.Table:
    """Create and configure the results table."""
    result_table = table.Table(title="Materialization Check Results")

    for column in TABLE_COLUMNS:
        result_table.add_column(column["name"], justify=column["justify"])

    return result_table


def get_datastacks_to_check() -> List[str]:
    """Get the list of datastacks to check."""
    if not DATASTACKS_TO_CHECK:
        datastacks_to_check = []
        for server in tqdm.tqdm(
            SERVERS_TO_CHECK, desc="Fetching datastacks from servers", ncols=80
        ):
            client = CAVEclient(datastack_name=None, server_address=server)
            datastacks_to_check.extend(sorted(client.info.get_datastacks()))
        return datastacks_to_check
    else:
        return sorted(DATASTACKS_TO_CHECK)


def main():
    """Main execution function."""
    print("\n")
    current_day = datetime.datetime.now().date()
    result_table = create_result_table()
    datastacks_to_check = get_datastacks_to_check()
    c = console.Console()

    for datastack in tqdm.tqdm(
        datastacks_to_check, desc="Checking datastacks", ncols=80
    ):
        (
            latest_version,
            expected_latest_version,
            latest_timestamp,
            server,
            server_version,
        ) = check_materialization(datastack)
        if latest_version is None:
            continue

        success = latest_version == expected_latest_version

        row_data = format_row_data(
            datastack=datastack,
            server=server,
            server_version=server_version,
            latest_version=latest_version,
            expected_latest_version=expected_latest_version,
            latest_timestamp=latest_timestamp,
            current_day=current_day,
            success=success,
        )

        result_table.add_row(*row_data)

    c.print("\n")
    c.print(result_table)


if __name__ == "__main__":
    main()
