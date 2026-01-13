# Excel Email Scheduler

A Python tool that reads Excel (.xlsx) files and sends personalized emails based on configurable templates. Supports multiple authentication methods including Basic SMTP, Gmail OAuth2, and Outlook OAuth2.

**[中文文档](docs/README-CN.md)**

## Features

- Read recipient data from Excel files (.xlsx)
- Send personalized emails using template placeholders
- Multiple authentication methods:
  - Basic SMTP (QQ Mail, 163 Mail, etc.)
  - Gmail OAuth2
  - Outlook/Office365 OAuth2
- Watch mode: Periodically check for new Excel files and send emails automatically
- Dry-run mode for testing without sending actual emails
- Configurable logging

## Requirements

- Python 3.10+
- Dependencies:
  ```
  pyyaml
  openpyxl
  ```

For OAuth2 authentication:
- Gmail: `google-auth-oauthlib`, `google-api-python-client`
- Outlook: `msal`, `requests`

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd excel-email-scheduler
   ```

2. Create a virtual environment and install dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install pyyaml openpyxl
   ```

3. For Gmail OAuth2:
   ```bash
   pip install google-auth-oauthlib google-api-python-client
   ```

4. For Outlook OAuth2:
   ```bash
   pip install msal requests
   ```

## Configuration

Copy `config.yaml` and modify according to your needs:

### Authentication Types

#### Basic SMTP (for QQ Mail, 163 Mail, etc.)

```yaml
auth_type: "basic"

smtp:
  host: "smtp.qq.com"
  port: 587
  use_tls: true
  username: "your_email@qq.com"
  password: "your_app_password"
```

#### Gmail OAuth2

```yaml
auth_type: "gmail_oauth2"

gmail_oauth2:
  credentials_file: "gmail_credentials.json"
  token_file: "gmail_token.json"
  scopes:
    - "https://www.googleapis.com/auth/gmail.send"
```

Setup steps:
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a project and enable Gmail API
3. Create OAuth2 credentials (Desktop app)
4. Download the JSON file and save as `gmail_credentials.json`

#### Outlook OAuth2

```yaml
auth_type: "outlook_oauth2"

outlook_oauth2:
  client_id: "your_client_id"
  client_secret: "your_client_secret"
  tenant_id: "common"
  token_cache_file: "outlook_token_cache.json"
```

Setup steps:
1. Go to [Azure Portal](https://portal.azure.com/)
2. Register a new application in Azure Active Directory
3. Configure API permissions (Mail.Send)
4. Create a client secret

### Email Template

```yaml
email:
  from_address: "your_email@example.com"
  subject: "Hello {Name}"
  body: |
    Hi {Name},

    Your order number is {OrderNumber}.

    Best regards,
    Your Team
```

Use `{ColumnName}` placeholders that match your Excel column headers.

### Workspace

```yaml
workspace:
  path: "./workspace"
```

Place your `.xlsx` files in this directory.

### Scheduler (Watch Mode)

```yaml
scheduler:
  enabled: false
  check_interval: 86400  # seconds (default: 1 day)
  processed_files_record: "./processed_files.json"
```

## Excel File Format

Your Excel file should have:
- First row: Column headers
- Columns containing "Leader" in the name will be used as email recipient addresses

Example:

| Name | Email | Leader Email | OrderNumber |
|------|-------|--------------|-------------|
| John | john@example.com | leader1@example.com | 12345 |
| Jane | jane@example.com | leader2@example.com | 12346 |

## Usage

### One-time Processing

Process all Excel files in the workspace once:

```bash
python excel-email-scheduler.py
```

### Dry Run Mode

Preview emails without sending:

```bash
python excel-email-scheduler.py --dry-run
```

### Watch Mode

Continuously monitor for new Excel files:

```bash
python excel-email-scheduler.py --watch
```

Or enable in config:
```yaml
scheduler:
  enabled: true
```

### Custom Config File

```bash
python excel-email-scheduler.py --config /path/to/config.yaml
```

### Command Line Options

| Option | Description |
|--------|-------------|
| `--config PATH` | Path to configuration file (default: config.yaml) |
| `--dry-run` | Preview emails without sending |
| `--watch` | Run in watch mode |

## How Watch Mode Works

1. On startup, checks for new/modified Excel files
2. Processes new files and sends emails
3. Records processed files in `processed_files.json`
4. Waits for the configured interval
5. Repeats the check

Files are tracked by their modification time and size. Modified files will be reprocessed.

Press `Ctrl+C` to gracefully stop the scheduler.

## Logging

Configure logging in `config.yaml`:

```yaml
logging:
  level: "INFO"  # DEBUG, INFO, WARNING, ERROR
  file: "./email_scheduler.log"
```

## License

MIT License
