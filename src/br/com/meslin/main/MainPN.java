package br.com.meslin.main;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import br.com.meslin.main.events.GPS;
import br.com.meslin.main.events.PM25;
import br.com.meslin.model.Region;
import com.espertech.esper.client.*;
import com.espertech.esper.event.map.MapEventBean;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import br.com.meslin.auxiliar.StaticLibrary;
import ckafka.data.Swap;
import ckafka.data.SwapData;
import main.java.application.ModelApplication;

public class MainPN extends ModelApplication implements UpdateListener {
	private Swap swap;
	private ObjectMapper objectMapper;
	private static EPRuntime cepRT;

	public MainPN() {
		this.objectMapper = new ObjectMapper();
		this.swap = new Swap(objectMapper);

		Configuration cepConfig = new Configuration();
		cepConfig.addEventType("GPS", GPS.class.getName());
		cepConfig.addEventType("PM25", PM25.class.getName());
		EPServiceProvider cep = EPServiceProviderManager.getProvider("myCEPEngine", cepConfig);
		cepRT = cep.getEPRuntime();

		EPAdministrator cepAdm = cep.getEPAdministrator();

		String cepStatement = "select 'Pouca poluição' as poluicao from PM25 where concentration < 1.0";
		String cepStatement2 = "select 'Muito poluído' as poluicao from PM25 where concentration > 1.0";

		String cepStatement3 = "select latitude, longitude from GPS.win:time_batch(1 min)";

		EPStatement cepInspector = cepAdm.createEPL(cepStatement);
		EPStatement cepInspector2 = cepAdm.createEPL(cepStatement2);
		EPStatement cepInspector3 = cepAdm.createEPL(cepStatement3);

		cepInspector.addListener(this);
		cepInspector2.addListener(this);
		cepInspector3.addListener(this);
	}

	public static void main(String[] args) {
		Map<String,String> env = new HashMap<String, String>();
		env.putAll(System.getenv());
		if(System.getenv("app.consumer.topics") == null) 			env.put("app.consumer.topics", "AppModel");
		if(System.getenv("app.consumer.auto.offset.reset") == null) env.put("app.consumer.auto.offset.reset", "latest");
		if(System.getenv("app.consumer.bootstrap.servers") == null) env.put("app.consumer.bootstrap.servers", "127.0.0.1:9092");
		if(System.getenv("app.consumer.group.id") == null) 			env.put("app.consumer.group.id", "gw-consumer");
		if(System.getenv("app.producer.bootstrap.servers") == null) env.put("app.producer.bootstrap.servers", "127.0.0.1:9092");
		if(System.getenv("app.producer.retries") == null) 			env.put("app.producer.retries", "3");
		if(System.getenv("app.producer.enable.idempotence") == null)env.put("app.producer.enable.idempotence", "true");
		if(System.getenv("app.producer.linger.ms") == null) 		env.put("app.producer.linger.ms", "1");
		if(System.getenv("app.producer.acks") == null) 				env.put("app.producer.acks", "all");
		try {
			StaticLibrary.setEnv(env);
		} catch (Exception e) {
			e.printStackTrace();
		}
		new MainPN();
	}

	private void sendGroupcastMessage(String message, String group) {
		//System.out.print("Mensagem groupcast. Entre com o número do grupo: ");
		//String group = keyboard.nextLine();
		//System.out.print("Entre com a mensagem: ");
		//String messageText = keyboard.nextLine();
		//String group = "10";
		//System.out.println(String.format("Enviando mensagem %s para o grupo %s.", message, group));
		
		try {
			sendRecord(createRecord("GroupMessageTopic", group, swap.SwapDataSerialization(createSwapData(message))));
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Error SendGroupCastMessage", e);
		}
	}

	@Override
	public void recordReceived(ConsumerRecord record) {
        this.logger.debug("Record Received " + record.value().toString());
        System.out.println(String.format("Mensagem recebida de %s", record.key()));
        
        try {
			SwapData data = swap.SwapDataDeserialization((byte[]) record.value());
			TypeReference<Map<String, String>> typeRef = new TypeReference<Map<String, String>>() {};
			Map<String, String> map = objectMapper.readValue(data.getMessage(), typeRef);
			String payload = map.get("payload").replaceAll("^\"|\"$", "").replace("\\", "");


			if (payload.contains("event_name")) {
				TypeReference<Map<String, Object>> typeRef2 = new TypeReference<Map<String, Object>>() {};
				Map<String, Object> eventDict = objectMapper.readValue(payload, typeRef2);
				Map<String, Object> eventData = (Map<String, Object>) eventDict.get("event_data");
				double concentration = (double) eventData.get("pm25");

				PM25 pmData = new PM25(concentration);
				System.out.println("Concentration " + concentration);
				cepRT.sendEvent(pmData);

			} else if (payload.contains("sensor_name")) {
				TypeReference<List<Map<String, Object>>> typeRef3 = new TypeReference<List<Map<String, Object>>>() {};
				List<Map<String, Object>> sensorDataList = objectMapper.readValue(payload, typeRef3);

				if (sensorDataList.size() > 6) {
					Map<String, Object> gpsDataArray = sensorDataList.get(7);
					List<Double> gpsList = (List<Double>) gpsDataArray.get("sensor_data");

					double latitude = gpsList.get(0);
					double longitude = gpsList.get(1);
					int region = 10;

					GPS gpsData = new GPS(latitude, longitude, region);

					cepRT.sendEvent(gpsData);
				}
			}
		} catch (NullPointerException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void update(EventBean[] newEvents, EventBean[] oldEvents) {
		Map<String, Object> props = ((MapEventBean) newEvents[0]).getProperties();
		System.out.println("Just got an CEP event: " + props);

		// Usa latitude e longitude para descobrir número da região
		int regionNumber;

		// Somente para processing node
		//sendGroupcastMessage(text, "10");
	}

}

