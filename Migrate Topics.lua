-- A resident script to run on a Clipsal 5500SHAC to push MQTT events to Cbus.

-- Tested with 5500SHAC firmware v1.6

-- Install this script as a resident script with a sleep interval of 5 seconds

-- **********************************************************************
-- MOSQUITTO DOCUMENTATION
-- https://flukso.github.io/lua-mosquitto/docs/
-- **********************************************************************


-- *************  Broker Connection Configuration  *******************
-- mqtt_broker = '10.1.20.50'
mqtt_broker = '10.1.20.36'
mqtt_username = 'cbus'
mqtt_password = 'M@!shaba100'
mqtt_userid = 'TopicMigrate'


-- *************  Broker Topic Configuration  *******************
mqtt_publish_topic = 'cbus/status'
mqtt_subscribe_topics = {"cbus/read/#"}


-- *************  Load MQTT library and create new client *******************
mqtt = require("mosquitto")
log(string.format("Mosquitto Version %s", mqtt.version()))
client = mqtt.new(mqtt_userid)


-- ************* On Connect Callback Function ***************
client.ON_CONNECT = function()
    
    log(string.format("Client Connected to %s", mqtt_broker));
    
    -- ************* Subscribe to all required topics ***************
  for idx = 1, table.maxn(mqtt_subscribe_topics)
	do
        mid = client:subscribe(mqtt_subscribe_topics[idx], 2);
        log(string.format("Subscribed to topic: %s", mqtt_subscribe_topics[idx]))   
	end
end


-- *********** On Disconnect Callback Function *************
client.ON_DISCONNECT = function()

    log(string.format("MQTT2CBUS - MQTT disconnected"))
    
    client:login_set(mqtt_username, mqtt_password);
	client:will_set (mqtt_lwt_topic, mqtt_lwt_offline, 1, true);    
	ConnectToBroker(mqtt_broker)

end


-- **************** On Message Callback Function ***************
client.ON_MESSAGE = function(mid, topic, payload)

    log(string.format("Received: %s %s", topic, payload))
  
  	parts = string.split(topic, "/")

  	if table.maxn(parts) == 6 then
			app = parts[4]
    	group = parts[5]
    	
    	log(string.format("SEND: %s/%u/%u -> %s", mqtt_publish_topic, app, group, payload))
    
			mqtt_msg = string.format('%s/%u/%u', mqtt_publish_topic, app, group)
      client:publish(mqtt_publish_topic, payload, 0, true)
    
    end
  
  
--    if not parts[5] then
--    log(string.format("Invalid MQTT message format: %s -> %s", topic, payload))

--    else
--    log(string.format("MQTT IN: %s / %s", topic, payload))
--    end
  

  
--    if not parts[6] then
--		log('MQTT2CBUS - MQTT error', 'Invalid message format')
		--log(string.format("Topic: %s -> Payload: %u", topic, payload))

--	end
end


-- ************** Connect to Broker Function **************
function ConnectToBroker(broker)

    local retries = 0
    
    while (client:connect(broker) ~= true)
    do
        retries = retries + 1
        os.sleep(2)
        
        if (retries == 5) then
            retries = 0
	        	log(string.format("Error connecting to broker '%s' . . . Retrying", broker))
        end
    end
end


client:login_set(mqtt_username, mqtt_password);
--client:will_set (mqtt_lwt_topic, mqtt_lwt_offline, 1, true);    
ConnectToBroker(mqtt_broker)

client:loop_forever()
