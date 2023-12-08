package br.com.meslin.main;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import br.com.meslin.main.events.AQI;
import br.com.meslin.main.events.GPS;
import br.com.meslin.main.events.PM25;
import br.com.meslin.main.events.TablePM25Event;
import com.espertech.esper.client.*;
import com.espertech.esper.event.arr.ObjectArrayEventBean;
import com.espertech.esper.event.bean.BeanEventBean;
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

		NowCast n = new NowCast();
		//NowCast.getCSVData();

		Configuration cepConfig = new Configuration();
		cepConfig.addEventType("GPS", GPS.class.getName());
		cepConfig.addEventType("PM25", PM25.class.getName());
		cepConfig.addEventType("AQI", AQI.class.getName());
		cepConfig.addEventType("TablePM25Event", TablePM25Event.class.getName());

		EPServiceProvider cep = EPServiceProviderManager.getProvider("myCEPEngine", cepConfig);
		cepRT = cep.getEPRuntime();

		EPAdministrator cepAdm = cep.getEPAdministrator();

		String createTableStatement = "create table PM25Table (region int primary key, avgConcentration double)";

		//String mergeEvents = "insert into AQI select g.region as region, avg(p.concentration) as concentration from GPS#time(5 min) g, PM25#time(5 min) p";
		String mergeEvents = "insert into AQI select g.region as region, p.concentration as concentration from GPS.win:time(1 min) g, PM25.win:time(1 min) p";

		//String cepStatement3 = "select region as region, avg(concentration) as concentration from AQI#time(1 min)";

		String upsertTableStatement = "on AQI as a merge PM25Table as t where a.region = t.region " +
				"when not matched then " +
				"insert select a.region as region, a.concentration as avgConcentration " +
				"when matched then " +
				"update set avgConcentration = (avgConcentration + a.concentration)/2";

		String selectTableEventStatement = "select * from TablePM25Event.win:time_batch(5 min)";

		//String selectTableStatement = "select region, avgConcentration from PM25Table";
		String selectTableStatement = "on TablePM25Event select avgConcentration from PM25Table group";

		EPStatement cepInspectorCreate = cepAdm.createEPL(createTableStatement);
		EPStatement cepInspectorMerge = cepAdm.createEPL(mergeEvents);
		//EPStatement cepInspector3 = cepAdm.createEPL(cepStatement3);
		EPStatement cepInspector3 = cepAdm.createEPL(selectTableEventStatement);
		EPStatement cepInspectorUpsert = cepAdm.createEPL(upsertTableStatement);
		EPStatement cepInspectorSelect = cepAdm.createEPL(selectTableStatement);

		cepInspectorCreate.addListener(this);
		cepInspectorMerge.addListener(this);
		cepInspector3.addListener(this);
		cepInspectorUpsert.addListener(this);
		cepInspectorSelect.addListener(this);
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
        //System.out.println(String.format("Mensagem recebida de %s", record.key()));
        
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
				//System.out.println("Concentration " + concentration);
				cepRT.sendEvent(pmData);

			} else if (payload.contains("sensor_name")) {
				TypeReference<List<Map<String, Object>>> typeRef3 = new TypeReference<List<Map<String, Object>>>() {};
				List<Map<String, Object>> sensorDataList = objectMapper.readValue(payload, typeRef3);

				if (sensorDataList.size() >= 8) {
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
		//Map<String, Object> props = ((MapEventBean) newEvents[0]).getProperties();
		if (newEvents[0] instanceof ObjectArrayEventBean) {
			Object[] propsArray = ((ObjectArrayEventBean) newEvents[0]).getProperties();
			Object lastProp = propsArray[propsArray.length-1];
			//for (Object prop : propsArray) {
			System.out.println("Just got a CEP event property: " + lastProp);
			//}

			//System.out.println("Just got an CEP event: " + props);

		}

		//System.out.println("Just got an CEP event: " + newEvents[0]);

		// Somente para processing node
		//sendGroupcastMessage(text, "10");
	}

}

