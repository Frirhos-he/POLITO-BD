package it.polito.bigdata.spark.example;

import java.io.Serializable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;

import scala.Tuple2;

public class SparkDriver {

public static void main(String[] args) {
  String inputMeetings;
  String inputUsers;
  String invitationsInput;
  String outputPathPart1;
  String outputPathPart2;
  
  inputMeetings = "Books.txt";
  inputUsers = "Users.txt";
  invitationsInput = "something.txt";
  outputPathPart1 = "outPart1/";
  outputPathPart2 = "outPart2/";
  
  // Create a configuration object and set the name of the application
  SparkConf conf = new SparkConf().setAppName("Spark Exam - Exercise #2");
  
  // Create a Spark Context object
  JavaSparkContext sc = new JavaSparkContext(conf);

  ////////////// PART 1 //////////////
  JavaPairRDD<String, Integer> usersBusiness = sc.textFile(inputUsers).filter(
     e->e.split(",")[4].equals("Business")
  ).mapToPair(e->new Tuple2<String, Integer>(e.split(",")[0], null)).cache();

  JavaRDD<String> meetings = sc.textFile(inputMeetings).cache();

  meetings.mapToPair(e->{
    String[] fields = e.split(",");
    String oid = fields[4];
    Double dur = Double.parseDouble(fields[3]);
    return new Tuple2<String, SumCountMinMax>(oid, new SumCountMinMax(dur,1,dur,dur));
  }).reduceByKey((a,b) -> {
     SumCountMinMax scmm = new SumCountMinMax(a.sumDur+b.sumDur,a.count+b.count,0.0,0.0);
      
     if(a.minDur<b.minDur) { scmm.minDur=a.minDur; }
     else { scmm.minDur=b.minDur; }

     if(a.maxDur>b.maxDur) { scmm.maxDur=a.maxDur; }
     else { scmm.maxDur = b.maxDur; }

     return scmm;
  })
  .join(usersBusiness)  // get only users with business plan
  .mapToPair(e->{
     String key = e._1();
     Double avg = (double) e._2()._1().sumDur / e._2()._1().count ;
     String value = avg + "," + e._2()._1().maxDur + "," + e._2()._1().minDur;
     return new Tuple2<String, String>(key, value);
  })
  .saveAsTextFile(outputPathPart1);

  /////////// PART 2 //////////
  JavaRDD<String> invitations = sc.textFile(invitationsInput);
  
  // calculate pairs (MID, nInvitations)
  JavaPairRDD<String, Integer> meetingInvitations = invitations.mapToPair(e->{
    String[] fields = e.split(",");
    return new Tuple2<String, Integer>(fields[0], 1);
  }).reduceByKey((a,b)->{
    return a+b;
  });

  // get the Business user that organized at least one meeting which
  JavaPairRDD<String, String> meetingOrg = meetings.mapToPair(e->{
    String[] fields = e.split(",");
    String oid = fields[4];
    String mid = fields[0];
    return new Tuple2<String, String>(oid, mid);
  })
  .join(usersBusiness)  // get only for business
  .mapToPair(e->{
    String mid = e._2()._1();
    String oid = e._1();
    return new Tuple2<String, String>(mid,oid);
  });

  meetingOrg.join(meetingInvitations)
  .mapToPair(e->{
     String oid = e._2()._1();
     String mid = e._1();
     Integer inv = e._2()._2();
     StatMeetings sm = new StatMeetings(0,0,0);
     if(inv<5) { sm.small = 1; }
     else if(inv<20) {sm.med = 1; }
     else { sm.large = 1; }
     return new Tuple2<String, StatMeetings>(oid, sm);
  }).reduceByKey((a,b) -> {
     return new StatMeetings(a.small+b.small, a.med+b.med, a.large+b.large);
  }).mapToPair(e->{
     String key = e._1();
     String value = e._2().small+","+e._2().med+","+e._2().large;
     return new Tuple2<String, String>(key,value);
  }).saveAsTextFile(outputPathPart2);
 
  sc.close();

}

   public class SumCountMinMax implements Serializable {
      public SumCountMinMax(double d, int i, double j, double k) {
         //TODO Auto-generated constructor stub
      }
      double sumDur;
      int count;
      double minDur;
      double maxDur;
   }

   public class StatMeetings implements Serializable {
      public StatMeetings(int i, int j, int k) {
         //TODO Auto-generated constructor stub
      }
      int small;
      int med;
      int large;
   }
}
