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
    console.log( 'EH: Ready.' )
  } catch ( exc ) { 
    console.error( 'Exception in MAIN run()', exc ) 
    process.exit( 0 )
  }
}

// ----------------------------------------------------------------------------
const onMessage = ( eventData ) => {
  try { 
    // eventhub delivers messages as bulk:
    for ( let record of extractLogArr( eventData ) ) {
      console.log( record )

      // if ( record.Type != 'ContainerLog' ) {
      //   console.log( record )
      // }

      // console.log( record.LogEntrySource +' '+ record.LogEntry )
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

