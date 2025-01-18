-- A resident script to run on a Clipsal 5500SHAC to push Cbus events to MQTT
-- Tested with 5500SHAC firmware v1.6
-- Install this script as a resident script with a sleep interval of 5 seconds


-- **********************************************************************
-- MOSQUITTO DOCUMENTATION
-- https://flukso.github.io/lua-mosquitto/docs/
-- **********************************************************************


--UDP Configuration
udp_host = '127.0.0.1'
udp_port = 5432


--Broker Configuration
mqtt_broker = '10.1.20.36'
mqtt_username = 'cbus'
mqtt_password = 'M@!shaba100'
mqtt_userid = 'CBUS2MQTT'

mqtt_lwt_topic = 'shac/cbus2ha/lwt'
mqtt_lwt_offline = 'offline'
mqtt_lwt_online = 'online'

mqtt_publish_topic = 'cbus/status'
mqtt_subscribe_topics = {}


-- load mqtt module
mqtt = require("mosquitto")

log(string.format("Mosquitto Version %s", mqtt.version()))


-- create new mqtt client
client = mqtt.new(mqtt_userid)


-- C-Bus events to MQTT local listener
server = require('socket').udp()
server:settimeout(1)
server:setsockname(udp_host, udp_port)
log(string.format("CBUS2MQTT - Opened UDP Socket on %s:%u", udp_host, udp_port))


client.ON_CONNECT = function(client1, userdata, flags, rc)
    
  log(string.format("CBUS2MQTT - Connected to %s -> %s", mqtt_broker, flags));
  client:publish(mqtt_lwt_topic, mqtt_lwt_online, 1, true)

end



client.ON_DISCONNECT = function()

  log(string.format("CBUS2MQTT - MQTT disconnected"))

  while (client:reconnect() ~= true)
  do
    log(string.format("Error reconnecting to broker . . . Retrying"))
    os.sleep(2)
  end
  
  log("reconnected")
  
end



client.ON_SUBSCRIBE = function()
    --log("Successfully subscribed to topic")
end



client.ON_MESSAGE = function(mid, topic, payload)
--	log(string.format("CBUS2MQTT - Received: %s %s", topic, payload))
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
--          	mqtt_ha_cbus = GetUserParam('Local Network', 'MQTT HA-CBUS NOCONNECT')
--	    			SetUserParam('Local Network', 'MQTT HA-CBUS NOCONNECT', mqtt_ha_cbus + 1)
	        	log(string.format("Error connecting to broker '%s' . . . Retrying", broker))
        end
    end
end



client:login_set(mqtt_username, mqtt_password);
client:will_set (mqtt_lwt_topic, mqtt_lwt_offline, 1, true);    

ConnectToBroker(mqtt_broker)
client:loop_start()




--function start_loop()
while true do
	cmd = server:receive()
	if cmd then
    --log(string.format('CBUS2MQTT - UDP Msg Recvd: %s', cmd))

    parts = string.split(cmd, "/")
    
    if table.maxn(parts) < 4 then
        log(string.format('Invalid UDP Msg Recvd: %s', data))
    else
        
      network = 254
      app = tonumber(parts[2])
            
      if (app == 228) then	-- Measurement
        device_id = tonumber(parts[3])
        channel_id = tonumber(parts[4])
        value = tonumber(parts[5])

        mqtt_msg = string.format('%s/%u/%u/%u/%s', mqtt_publish_topic, app, device_id, channel_id, "measurement")
        client:publish(mqtt_msg .. "/state", value, 0, false)

      elseif (app == 203) then	-- Enable Control
        group = tonumber(parts[3])
        level = tonumber(parts[4])
        state = (level ~= 0) and "on" or "off"

        mqtt_msg = string.format('%s/%u/%u', mqtt_publish_topic, app, group)
        client:publish(mqtt_msg .. "/state", value, 0, true)
        client:publish(mqtt_msg .. "/level", value, 0, true)

      elseif (app == 250) then	-- User Parameters
        group = tonumber(parts[3])
        level = tonumber(parts[4])

        if (group == 2) then
          log(string.format('CBUS2MQTT - UDP Msg Recvd: %s %u', cmd, level))

          -- ********* Publish Old format until cut across ************
          mqtt_msg = string.format('%s/heartbeat', mqtt_publish_topic)
          client:publish(mqtt_msg, level, 1, false)

        else
          -- **** Publish NEW Topic ***
          mqtt_msg = string.format('%s/%u/%u/level', mqtt_publish_topic, app, group)
          client:publish(mqtt_msg, value, 0, true)

        end

      elseif (app == 255) then	-- Unit Parameters
        device = tonumber(parts[3])
        channel = tonumber(parts[4])
        value = tonumber(parts[5])

        mqtt_msg = string.format('%s/%u/%u/%u/value', mqtt_publish_topic, app, device, channel)
        client:publish(mqtt_msg, value, 0, true)
                
      else
        group = tonumber(parts[3])
        level = tonumber(parts[4])
        state = (level ~= 0) and "on" or "off"

        mqtt_msg = string.format('%s/%u/%u', mqtt_publish_topic, app, group)
        client:publish(mqtt_msg .. "/state", state, 0, true)
        client:publish(mqtt_msg .. "/level", level, 0, true)
        log(string.format('%s -> state: %s / level: %u', mqtt_msg, state, level))
        
      end
	  end
  end
end