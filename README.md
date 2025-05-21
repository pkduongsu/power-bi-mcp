# PowerBI MCP Server

A Model Context Protocol (MCP) server for interacting with Microsoft PowerBI REST API. This server provides comprehensive tools for data cleaning, transformation, analysis, and visualization within PowerBI.

## Features

### Resources
- List PowerBI workspaces
- Browse datasets and reports in workspaces
- Access dataset schemas and metadata

### Tools
- **Data Operations**
  - Get workspaces, datasets, reports
  - Get detailed dataset information
  - Query datasets with DAX
  - Refresh datasets
  - Analyze data quality
  - Export data to CSV format

- **Visualization & Reporting**
  - Create and clone reports
  - Get report pages and structure
  - Generate visualization suggestions
  - Create visualization-ready data
  - Generate DAX measures for specific chart types
  - Export chart data for external tools

- **Data Transformation**
  - Create calculated columns
  - Create measures
  - Execute custom DAX expressions

- **Report Management**
  - Create new reports
  - List existing reports
  - Get refresh history

- **Analysis**
  - Analyze data quality across tables
  - Generate insights from datasets
  - Performance optimization suggestions

### Prompts
- Dataset analysis guidance
- Dashboard creation assistance
- Data cleaning workflows
- DAX optimization help
- Visualization design recommendations
- Dashboard layout guidance
- Chart-specific best practices

## Prerequisites

1. **PowerBI Service Account**: You need access to PowerBI Pro or Premium
2. **Azure App Registration**: Register an application in Azure AD with PowerBI API permissions
3. **API Permissions**: Grant the following permissions to your Azure app:
   - Dataset.Read.All
   - Dataset.ReadWrite.All
   - Report.Read.All
   - Report.ReadWrite.All
   - Workspace.Read.All

## Setup

### 1. Azure App Registration

1. Go to [Azure Portal](https://portal.azure.com)
2. Navigate to "Azure Active Directory" > "App registrations"
3. Click "New registration"
4. Configure:
   - Name: PowerBI MCP Server
   - Supported account types: Accounts in this organizational directory only
   - Redirect URI: Not needed for this setup
5. After creation, note down:
   - Application (client) ID
   - Directory (tenant) ID
6. Go to "Certificates & secrets" and create a new client secret
7. Go to "API permissions" and add PowerBI Service permissions

### 2. Environment Variables

Create a `.env` file or set these environment variables:

```bash
POWERBI_TENANT_ID=your-tenant-id
POWERBI_CLIENT_ID=your-client-id
POWERBI_CLIENT_SECRET=your-client-secret
```

### 3. Installation

```bash
# Clone the repository
git clone <repository-url>
cd powerbi-mcp-server

# Install dependencies
pip install mcp[cli] httpx pandas openpyxl

# Or using uv
uv add "mcp[cli]" httpx pandas openpyxl
```

### 4. Running the Server

#### Development Mode
```bash
mcp dev powerbi_server.py
```

#### Claude Desktop Integration
```bash
mcp install powerbi_server.py --name "PowerBI Server"
```

#### Direct Execution
```bash
python powerbi_server.py
```

## Usage Examples

### Analyzing a Dataset
1. Use the `get_workspaces` tool to find your workspace ID
2. Use `get_datasets` to find the dataset you want to analyze
3. Use `get_dataset` to get detailed information about the specific dataset
4. Use `get_dataset_schema` to understand available tables and columns
5. Use `analyze_data_quality` to get insights about data completeness
6. Use the "analyze_dataset_prompt" for comprehensive analysis guidance

### Creating Visualizations
1. Use `generate_visualization_suggestions` to get chart recommendations
2. Use `create_dax_for_visualization` to generate optimized measures
3. Use `create_visualization_ready_data` to format data for charts
4. Use `create_report` to create PowerBI reports
5. Export data for external visualization tools if needed
6. Use visualization prompts for design guidance

### Data Transformation
1. Use `export_data_to_csv` to extract raw data
2. Use `create_calculated_column` to add derived fields
3. Use `create_measure` for aggregations
4. Use the "data_cleaning_prompt" for transformation guidance

## API Reference

### Authentication
The server uses OAuth 2.0 client credentials flow for authentication. Tokens are automatically refreshed as needed.

### Error Handling
All tools return a standardized response format:
```json
{
  "success": boolean,
  "data": object,  // Present on success
  "error": string  // Present on failure
}
```

### Rate Limiting
The PowerBI API has rate limits. The server handles basic retry logic, but you may need to implement additional throttling for high-volume operations.

## Troubleshooting

### Authentication Issues
- Verify your Azure app registration has the correct permissions
- Ensure the client secret hasn't expired
- Check that the tenant ID is correct

### API Permissions
- Make sure your app has admin consent for PowerBI API permissions
- Verify the service principal has access to the required workspaces

### Data Access
- Ensure your Azure app has been granted access to PowerBI workspaces
- Check if datasets require specific permissions for programmatic access

## Security Considerations

1. **Secrets Management**: Never commit credentials to version control
2. **Least Privilege**: Only grant necessary PowerBI permissions
3. **Token Security**: Tokens are stored in memory and automatically refreshed
4. **Network Security**: Use HTTPS in production deployments

## Contributing

Contributions are welcome! Please ensure you:
1. Follow the existing code style
2. Add appropriate error handling
3. Update documentation for new features
4. Test thoroughly with different PowerBI configurations

## License

MIT License - see LICENSE file for details.