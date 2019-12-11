package com.uek.bigdata.mockData;

import com.uek.bigdata.utils.DateUtils;
import com.uek.bigdata.utils.StringUtils;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.Random;

/**
 * 类名称: MockRealTimeData
 * 类描述:
 *
 * @author 武建谦
 * @Time 2018/11/29 19:48
 */
public class MockRealTimeData extends Thread {

    private static final Random random = new Random();

    private static final String[] locations = new String[]{"鲁","京","京","京","沪","京","京","深","京","京"};

    private Producer<Integer, String> producer;

    public MockRealTimeData() {
        producer = new Producer<Integer, String>(createProducerConfig());
    }

    private ProducerConfig createProducerConfig() {
        Properties props = new Properties();
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", "hadoop:9092");
        return new ProducerConfig(props);
    }
    @Override
    public void run() {
        while(true) {
            String date = DateUtils.getTodayDate();
            String baseActionTime = date + " " + StringUtils.complementStr(random.nextInt(24)+"");
            baseActionTime = date + " " + StringUtils.complementStr((Integer.parseInt(baseActionTime.split(" ")[1])+1)+"");
            String actionTime = baseActionTime + ":" + StringUtils.complementStr(random.nextInt(60)+"") + ":" + StringUtils.complementStr(random.nextInt(60)+"");
            String monitorId = StringUtils.complementStr(4, random.nextInt(9)+"");
            String carNum = locations[random.nextInt(10)] + (char)(65+random.nextInt(26))+StringUtils.complementStr(5,random.nextInt(99999)+"");
            String speed = random.nextInt(260)+"";
            String roadId = random.nextInt(50)+1+"";
            String cameraId = StringUtils.complementStr(5, random.nextInt(15)+"");
            String areaId = StringUtils.complementStr(2,random.nextInt(8)+"");
            producer.send(new KeyedMessage<Integer, String>("RoadRealTimeLog","2018-11-30"+"\t"+"0007"+"\t"+"00012"+"\t"+"京W83100"+"\t"+"2018-11-30 07:51:19	28"+"\t"+"18"+"04"));
            //数据格式：
            producer.send(new KeyedMessage<Integer, String>("RoadRealTimeLog", date+"\t"+monitorId+"\t"+cameraId+"\t"+carNum + "\t" + actionTime + "\t" + speed + "\t" + roadId + "\t" + areaId));

            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 启动Kafka Producer
     * @param args
     */
    public static void main(String[] args) {
        MockRealTimeData producer = new MockRealTimeData();
        producer.start();
    }

}
