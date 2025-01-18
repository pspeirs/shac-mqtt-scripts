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
mqtt_userid = 'MQTT2CBUS'


-- *************  Broker Topic Configuration  *******************
mqtt_lwt_topic = 'shac/ha2cbus/lwt'
mqtt_lwt_offline = 'offline'
mqtt_lwt_online = 'online'

mqtt_subscribe_topics = {"cbus/cmd/#", "cbus/read/heartbeat"}


-- *************  Load MQTT library and create new client *******************
mqtt = require("mosquitto")
log(string.format("Starting MQTT2CBUS - Mosquitto Version %s", mqtt.version()))
client = mqtt.new(mqtt_userid)

SetUserParam('Local Network', 'MQTT HA-CBUS NOCONNECT', 0)


-- ************* On Connect Callback Function ***************
client.ON_CONNECT = function()
    
  log(string.format("MQTT2CBUS - MQTT Client Connected to %s", mqtt_broker));
  SetUserParam('Local Network', 'MQTT HA-CBUS NOCONNECT', 0)
  client:publish(mqtt_lwt_topic, mqtt_lwt_online, 1, true)
    
    -- ************* Subscribe to all required topics ***************
  for idx = 1, table.maxn(mqtt_subscribe_topics)
	do
    mid = client:subscribe(mqtt_subscribe_topics[idx], 2);
    log(string.format("Subscribed to topic: %s", mqtt_subscribe_topics[idx]))   
	end
    
	-- ************* Publish to LWT topic ***************
  client:publish(mqtt_lwt_topic, mqtt_lwt_online, 1, false)

end


-- *********** On Disconnect Callback Function *************
client.ON_DISCONNECT = function()

  log(string.format("MQTT2CBUS - MQTT disconnected"))
    
  client:login_set(mqtt_username, mqtt_password);
	client:will_set (mqtt_lwt_topic, mqtt_lwt_offline, 1, true);    
	ConnectToBroker(mqtt_broker)

  -- ********* Loop to keep trying a reconnection every 10 seconds ********
  --    while (client:reconnect() ~= true)
  --    do
  --        log(string.format("Error reconnecting to broker . . . Retrying"))
  --        os.sleep(10)
  --    end
end


-- **************** On Message Callback Function ***************
client.ON_MESSAGE = function(mid, topic, payload)

  log(string.format("MQTT2CBUS - Received: %s %s", topic, payload))

  parts = string.split(topic, "/")

  -- ******* Check if message is a ping then update the relevant user param
  if (topic == "cbus/read/heartbeat") then
    
    if (GetUserParam('Local Network', 'MQTT Ping') == payload) then
      SetUserParam('Local Network', 'MQTT State', 0)
      log("Matching timestamp: %s")
    else
      SetUserParam('Local Network', 'MQTT State', -1)
      log("Mismatch timestamp")
    end
    return
  end
  
  

  if not parts[5] then
    log(string.format("Invalid MQTT message format: %s -> %s", topic, payload))

  else

    log(string.format("MQTT IN: %s / %s", topic, payload))
  end
  

  
  --if not parts[6] then
  if table.maxn(parts) ~= 5 then
		log('MQTT2CBUS - MQTT error', 'Invalid message format')

  elseif parts[5] == "state" then

    if payload == "on" then
        SetCBusLevel(0, parts[3], parts[4], 255, 0)
    elseif payload == "off" then
        SetCBusLevel(0, parts[3], parts[4], 0, 0)
    else
        log(string.format("Unknown Message: %s %s", topic, payload))
    end
    
  elseif parts[5] == "level" then
  
    if payload == "on" then
      SetCBusLevel(0, parts[3], parts[4], 255)
          
    elseif payload == "off" then
      SetCBusLevel(0, parts[3], parts[4], 0)

    elseif tonumber(payload) then
      ramp = string.split(payload, ",")
      num = math.floor(ramp[1] + 0.5)
      if num and num < 256 then
        if ramp[2] ~= nil and tonumber(ramp[2]) > 1 then
          SetCBusLevel(0, parts[3], parts[4], num, ramp[2])
          log(string.format("Setting Level: cbus/cmd/%u/%u Lvl: %u Ramp: %u", parts[3], parts[4], num, ramp[2]))
        else
          SetCBusLevel(0, parts[3], parts[4], num, 0)
          log(string.format("Setting Level: cbus/cmd/%u/%u Lvl: %u Ramp: %u", parts[3], parts[4], num, 0))
                  
        end
      end
      
    else
      log(string.format("Unknown Message: %s %s", topic, payload))
    end

  elseif parts[5] == "measurement" then
    SetCBusMeasurement(0, parts[3], parts[4], (payload / 10), 0)
  

  elseif parts[5] == "cover" then
      
    log(string.format("Cover Message: %s %s", topic, payload))
  
    if payload == "open" then
      log(string.format("OPEN Received: 0/%s/%s - %s ", parts[3], parts[4], 255))
      SetCBusLevel(0, parts[3], parts[4], 255, 0)
          
    elseif payload == "close" then
      log(string.format("CLOSE Received: 0/%s/%s - %s", parts[3], parts[4], 0))
      SetCBusLevel(0, parts[3], parts[4], 0, 0)
          
    elseif payload == "stop" then
      log(string.format("STOP Received: 0/%s/%s - %s", parts[3], parts[4], 249))
      SetCBusLevel(0, parts[3], parts[4], 249, 0)
          
    else
      log(string.format("Unknown Message: %s %s", topic, payload))
          
    end
	end
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
      mqtt_ha_cbus = GetUserParam('Local Network', 'MQTT HA-CBUS NOCONNECT')
      SetUserParam('Local Network', 'MQTT HA-CBUS NOCONNECT', mqtt_ha_cbus + 1)
      log(string.format("Error connecting to broker '%s' . . . Retrying", broker))
    end
  end
end


client:login_set(mqtt_username, mqtt_password);
client:will_set (mqtt_lwt_topic, mqtt_lwt_offline, 1, true);    
ConnectToBroker(mqtt_broker)

client:loop_forever()
