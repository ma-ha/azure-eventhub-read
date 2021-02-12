# azure-eventhub-read
Helper: Just read and dumps messages from Azure Event Hub

# Usage:

Prepage:

   npm install @azure/event-hubs
   export EH_CONN_STR=get_connection_string_from_azure
   export EH_NAME=the_eh_name

Dump log records:

    node eh-print-messages.js

Collect some statistics:

    node eh-print-stats.js