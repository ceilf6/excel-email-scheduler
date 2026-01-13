#!/usr/bin/env python3
"""
Excel Email Scheduler

This script reads all xlsx files in the workspace directory and sends emails
based on a configurable template. Email addresses are extracted from columns
containing 'Leader' in their names.

Usage:
    python excel-email-scheduler.py [--config CONFIG_PATH] [--dry-run]
"""

import argparse
import logging
import os
import re
import smtplib
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

import yaml
from openpyxl import load_workbook


def setup_logging(config: dict) -> logging.Logger:
    """
    Setup logging based on configuration.

    Args:
        config: Configuration dictionary containing logging settings

    Returns:
        Configured logger instance
    """
    log_config = config.get("logging", {})
    log_level = getattr(logging, log_config.get("level", "INFO").upper(), logging.INFO)
    log_file = log_config.get("file", "")

    logger = logging.getLogger("email_scheduler")
    logger.setLevel(log_level)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_format = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)

    # File handler (optional)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        file_format = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
        file_handler.setFormatter(file_format)
        logger.addHandler(file_handler)

    return logger


def load_config(config_path: str) -> dict:
    """
    Load configuration from YAML file.

    Args:
        config_path: Path to the configuration file

    Returns:
        Configuration dictionary

    Raises:
        FileNotFoundError: If config file doesn't exist
        yaml.YAMLError: If config file is invalid
    """
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def find_xlsx_files(workspace_path: str) -> list[Path]:
    """
    Find all xlsx files in the workspace directory.

    Args:
        workspace_path: Path to the workspace directory

    Returns:
        List of Path objects for xlsx files
    """
    workspace = Path(workspace_path)
    if not workspace.exists():
        raise FileNotFoundError(f"Workspace directory not found: {workspace_path}")

    xlsx_files = list(workspace.glob("*.xlsx"))
    # Filter out temporary files (starting with ~$)
    xlsx_files = [f for f in xlsx_files if not f.name.startswith("~$")]
    return xlsx_files


def extract_placeholders(template: str) -> list[str]:
    """
    Extract placeholder names from a template string.

    Args:
        template: Template string containing {placeholder} patterns

    Returns:
        List of placeholder names
    """
    return re.findall(r"\{(\w+)\}", template)


def fill_template(template: str, row_data: dict[str, Any]) -> str:
    """
    Fill a template string with values from row data.

    Args:
        template: Template string containing {placeholder} patterns
        row_data: Dictionary mapping column names to values

    Returns:
        Filled template string
    """
    result = template
    for key, value in row_data.items():
        placeholder = "{" + key + "}"
        # Convert value to string, handle None values
        str_value = str(value) if value is not None else ""
        result = result.replace(placeholder, str_value)
    return result


def find_leader_columns(headers: list[str]) -> list[str]:
    """
    Find all column names containing 'Leader' (case-insensitive).

    Args:
        headers: List of column header names

    Returns:
        List of column names containing 'Leader'
    """
    return [h for h in headers if h and "leader" in h.lower()]


def read_xlsx_data(file_path: Path, logger: logging.Logger) -> list[dict[str, Any]]:
    """
    Read data from an xlsx file.

    Args:
        file_path: Path to the xlsx file
        logger: Logger instance

    Returns:
        List of dictionaries, each representing a row with column names as keys
    """
    logger.info(f"Reading file: {file_path}")

    wb = load_workbook(file_path, read_only=True, data_only=True)
    ws = wb.active

    rows = list(ws.iter_rows(values_only=True))

    if not rows:
        logger.warning(f"File {file_path} is empty")
        return []

    # First row is headers
    headers = [str(h).strip() if h else f"Column_{i}" for i, h in enumerate(rows[0])]
    logger.debug(f"Headers: {headers}")

    # Convert remaining rows to dictionaries
    data = []
    for row_idx, row in enumerate(rows[1:], start=2):
        row_dict = {}
        for col_idx, value in enumerate(row):
            if col_idx < len(headers):
                row_dict[headers[col_idx]] = value
        data.append({"row_number": row_idx, "data": row_dict})

    logger.info(f"Read {len(data)} rows from {file_path}")
    wb.close()
    return data


def create_email_message(
    from_addr: str,
    to_addr: str,
    subject: str,
    body: str
) -> MIMEMultipart:
    """
    Create an email message.

    Args:
        from_addr: Sender email address
        to_addr: Recipient email address
        subject: Email subject
        body: Email body

    Returns:
        MIMEMultipart email message
    """
    msg = MIMEMultipart()
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))
    return msg


def send_email(
    smtp_config: dict,
    from_addr: str,
    to_addr: str,
    message: MIMEMultipart,
    logger: logging.Logger,
    dry_run: bool = False
) -> bool:
    """
    Send an email using SMTP.

    Args:
        smtp_config: SMTP configuration dictionary
        from_addr: Sender email address
        to_addr: Recipient email address
        message: Email message to send
        logger: Logger instance
        dry_run: If True, don't actually send the email

    Returns:
        True if email was sent successfully, False otherwise
    """
    if dry_run:
        logger.info(f"[DRY RUN] Would send email to: {to_addr}")
        logger.debug(f"[DRY RUN] Subject: {message['Subject']}")
        return True

    try:
        host = smtp_config["host"]
        port = smtp_config["port"]
        use_tls = smtp_config.get("use_tls", True)
        username = smtp_config["username"]
        password = smtp_config["password"]

        if use_tls:
            server = smtplib.SMTP(host, port)
            server.starttls()
        else:
            server = smtplib.SMTP(host, port)

        server.login(username, password)
        server.sendmail(from_addr, to_addr, message.as_string())
        server.quit()

        logger.info(f"Email sent successfully to: {to_addr}")
        return True

    except smtplib.SMTPException as e:
        logger.error(f"Failed to send email to {to_addr}: {e}")
        return False


def is_valid_email(email: str) -> bool:
    """
    Check if a string is a valid email address.

    Args:
        email: String to validate

    Returns:
        True if valid email format, False otherwise
    """
    if not email or not isinstance(email, str):
        return False
    # Simple email validation regex
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return bool(re.match(pattern, email.strip()))


def process_xlsx_file(
    file_path: Path,
    config: dict,
    logger: logging.Logger,
    dry_run: bool = False
) -> dict[str, int]:
    """
    Process a single xlsx file and send emails.

    Args:
        file_path: Path to the xlsx file
        config: Configuration dictionary
        logger: Logger instance
        dry_run: If True, don't actually send emails

    Returns:
        Dictionary with statistics (sent, failed, skipped)
    """
    stats = {"sent": 0, "failed": 0, "skipped": 0}

    # Read xlsx data
    try:
        rows = read_xlsx_data(file_path, logger)
    except Exception as e:
        logger.error(f"Failed to read file {file_path}: {e}")
        return stats

    if not rows:
        return stats

    # Get email configuration
    email_config = config["email"]
    smtp_config = config["smtp"]
    subject_template = email_config["subject"]
    body_template = email_config["body"]
    from_addr = email_config["from_address"]

    # Get headers from first row's data
    if rows:
        headers = list(rows[0]["data"].keys())
        leader_columns = find_leader_columns(headers)

        if not leader_columns:
            logger.warning(f"No 'Leader' columns found in {file_path}")
            return stats

        logger.info(f"Found Leader columns: {leader_columns}")

    # Process each row
    for row_info in rows:
        row_num = row_info["row_number"]
        row_data = row_info["data"]

        # Fill templates with row data
        subject = fill_template(subject_template, row_data)
        body = fill_template(body_template, row_data)

        # Send to all Leader email addresses in this row
        for leader_col in leader_columns:
            email_addr = row_data.get(leader_col)

            if not is_valid_email(str(email_addr) if email_addr else ""):
                logger.debug(
                    f"Row {row_num}: Skipping invalid email in column '{leader_col}': {email_addr}"
                )
                stats["skipped"] += 1
                continue

            email_addr = str(email_addr).strip()

            # Create and send email
            message = create_email_message(from_addr, email_addr, subject, body)

            logger.info(f"Row {row_num}: Sending email to {email_addr} (column: {leader_col})")

            if send_email(smtp_config, from_addr, email_addr, message, logger, dry_run):
                stats["sent"] += 1
            else:
                stats["failed"] += 1

    return stats


def main():
    """Main entry point for the email scheduler."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Send emails based on xlsx data and configurable templates"
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview emails without sending"
    )
    args = parser.parse_args()

    # Load configuration
    try:
        config = load_config(args.config)
    except FileNotFoundError:
        print(f"Error: Configuration file not found: {args.config}")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"Error: Invalid configuration file: {e}")
        sys.exit(1)

    # Setup logging
    logger = setup_logging(config)

    # Determine dry run mode (CLI argument takes precedence)
    dry_run = args.dry_run or config.get("dry_run", False)

    if dry_run:
        logger.info("Running in DRY RUN mode - no emails will be sent")

    # Find xlsx files
    workspace_path = config.get("workspace", {}).get("path", "./workspace")

    try:
        xlsx_files = find_xlsx_files(workspace_path)
    except FileNotFoundError as e:
        logger.error(str(e))
        sys.exit(1)

    if not xlsx_files:
        logger.warning(f"No xlsx files found in {workspace_path}")
        sys.exit(0)

    logger.info(f"Found {len(xlsx_files)} xlsx file(s) to process")

    # Process each xlsx file
    total_stats = {"sent": 0, "failed": 0, "skipped": 0}

    for xlsx_file in xlsx_files:
        logger.info(f"Processing: {xlsx_file.name}")
        stats = process_xlsx_file(xlsx_file, config, logger, dry_run)

        for key in total_stats:
            total_stats[key] += stats[key]

    # Print summary
    logger.info("=" * 50)
    logger.info("Processing Complete!")
    logger.info(f"Total emails sent: {total_stats['sent']}")
    logger.info(f"Total emails failed: {total_stats['failed']}")
    logger.info(f"Total emails skipped: {total_stats['skipped']}")

    if total_stats["failed"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
