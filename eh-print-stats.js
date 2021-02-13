const { EventHubClient, EventPosition } = require( '@azure/event-hubs' )

let ehConnStr = null
let ehName    = null

run()

// ----------------------------------------------------------------------------
async function run() {
  try {    
    if ( process.env[ 'EH_CONN_STR' ] && process.env[ 'EH_NAME' ] ) {
      ehConnStr = process.env[ 'EH_CONN_STR' ] 
      ehName    = process.env[ 'EH_NAME' ] 
    } else {
      console.log( 'ERROR: Environment variable EH_CONN_STR or EH_NAME not set.' )
      process.exit( 0 ) 
    }
    
    console.log( 'EH: Start...' )
    const eventHub = EventHubClient.createFromConnectionString( ehConnStr, ehName )

    console.log( 'EH: Get partitions...' )
    const allPartitionIds = await eventHub.getPartitionIds()
    
    console.log( 'EH: Starting receivers...' )
    for ( let partition of allPartitionIds ) {
      console.log( 'EH: ... start receiver on partition '+partition )
      const rcvOpts =  { eventPosition: EventPosition.fromEnqueuedTime( Date.now() ) }
      receiveHandler = eventHub.receive( partition, onMessage, onErr, rcvOpts )  
    }
    console.log( 'EH: Ready. Collecting stats .... (print out every 60 sec)' )
  } catch ( exc ) { 
    console.error( 'Exception in MAIN run()', exc ) 
    process.exit( 0 )
  }
}


// ----------------------------------------------------------------------------
let sources = {}
let logtype = {}

// print out stats from tome to time
setInterval( () => { console.log( sources, logtype ) }, 60000 ) 


const onMessage = ( eventData ) => {
  try { 
    // eventhub delivers messages as bulk:
    for ( let record of extractLogArr( eventData ) ) {
      if ( ! sources[ record.LogEntrySource ] ) {
        sources[ record.LogEntrySource ] = 1
      } else {
        sources[ record.LogEntrySource ]++
      }
      if ( ! logtype[ record.Type ] ) {
        logtype[ record.Type ] = 1
      } else {
        logtype[ record.Type ]++
      }
    }
  } catch ( e ) {  log.error( 'EH receive', e ) }
}

// ----------------------------------------------------------------------------

const onErr = ( error ) => {
  log.error( 'EH: Error when receiving message: ', error )
}

// ----------------------------------------------------------------------------
// helper
function extractLogArr( eventData ) {
  if ( eventData.body && eventData.body.records ) {
    return eventData.body.records
  } else {
    return []
  }
}

