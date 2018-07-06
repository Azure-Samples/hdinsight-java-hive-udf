package com.microsoft.example; /**
 * Created by rabaner on 6/13/2016.
 */
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


@Description(
        name="SimpleUDFExample",
        value="returns string which can be cast to hive timestamp"
)
public class timestampconv extends UDF {

    public String evaluate(String  input[]) {
        if(input == null || input.length!=2) return null;
        String return_string=null;
        if(input[1].toLowerCase().contains("y/m/d")){
            String pattern = "(.*)/(.*)/(.*)";
            Pattern r = Pattern.compile(pattern);
            Matcher m = r.matcher(input[0]);
            if (m.find( )) {
                String year = m.group(1);
                String month = m.group(2);
                String date = m.group(3);
                //Two digits are always prepended with 19 in sql timestamp
                if(year.length()==2){
                    year = "19"+year;
                }
                return_string = year+"-"+month+"-"+date+" 00:00:00";
            } else {
                System.out.println("NO MATCH");
            }
        }

        if(input[1].toLowerCase().contains("y/d/m")){
            String pattern = "(.*)/(.*)/(.*)";
            Pattern r = Pattern.compile(pattern);
            Matcher m = r.matcher(input[0]);
            if (m.find( )) {
                String year = m.group(1);
                String date = m.group(2);
                String month = m.group(3);
                //Two digits are always prepended with 19 in sql timestamp
                if(year.length()==2){
                    year = "19"+year;
                }
                return_string = year+"-"+month+"-"+date+" 00:00:00";
            }
        }

        if(input[1].toLowerCase().contains("m/d/y")){
            String pattern = "(.*)/(.*)/(.*)";
            Pattern r = Pattern.compile(pattern);
            Matcher m = r.matcher(input[0]);
            if (m.find( )) {
                String month = m.group(1);
                String date = m.group(2);
                String year = m.group(3);
                //Two digits are always prepended with 19 in sql timestamp
                if(year.length()==2){
                    year = "19"+year;
                }
                return_string = year+"-"+month+"-"+date+" 00:00:00";
            }
        }

        if(input[1].toLowerCase().contains("m/y/d")){
            String pattern = "(.*)/(.*)/(.*)";
            Pattern r = Pattern.compile(pattern);
            Matcher m = r.matcher(input[0]);
            if (m.find( )) {
                String month = m.group(1);
                String year = m.group(2);
                String date = m.group(3);
                //Two digits are always prepended with 19 in sql timestamp
                if(year.length()==2){
                    year = "19"+year;
                }
                return_string = year+"-"+month+"-"+date+" 00:00:00";
            }
        }

        if(input[1].toLowerCase().contains("d/y/m")){
            String pattern = "(.*)/(.*)/(.*)";
            Pattern r = Pattern.compile(pattern);
            Matcher m = r.matcher(input[0]);
            if (m.find( )) {
                String date = m.group(1);
                String year = m.group(2);
                String month = m.group(3);
                //Two digits are always prepended with 19 in sql timestamp
                if(year.length()==2){
                    year = "19"+year;
                }
                return_string = year+"-"+month+"-"+date+" 00:00:00";
            }
        }
        if(input[1].toLowerCase().contains("d/m/y")){
            String pattern = "(.*)/(.*)/(.*)";
            Pattern r = Pattern.compile(pattern);
            Matcher m = r.matcher(input[0]);
            if (m.find( )) {
                String date = m.group(1);
                String month = m.group(2);
                String year = m.group(3);
                //Two digits are always prepended with 19 in sql timestamp
                if(year.length()==2){
                    year = "19"+year;
                }
                return_string = year+"-"+month+"-"+date+" 00:00:00";
            }
        }

		if(input[1].toLowerCase().contains("ddmmyyyy")){
            String pattern = "(0[1-9]|[1-2][0-9]|31(?!(?:0[2469]|11))|30(?!02))(0[1-9]|1[0-2])([12]\\d{3})";
            Pattern r = Pattern.compile(pattern);
            Matcher m = r.matcher(input[0]);
            if (m.find( )) {
                String date = m.group(1);
                String month = m.group(2);
                String year = m.group(3);
                return_string = year+"-"+month+"-"+date+" 00:00:00";
            }
        }
        if(input[1].toLowerCase().contains("y-m-d")){
            String pattern = "(.*)-(.*)-(.*)";
            Pattern r = Pattern.compile(pattern);
            Matcher m = r.matcher(input[0]);
            if (m.find( )) {
                String year = m.group(1);
                String month = m.group(2);
                String date = m.group(3);
                //Two digits are always prepended with 19 in sql timestamp
                if(year.length()==2){
                    year = "19"+year;
                }
                return_string = year+"-"+month+"-"+date+" 00:00:00";
            }
        }

        //yyyy-mm-ddthh:mm:ss[.mmm]
        if(input[1].toLowerCase().contains("yyyy-mm-ddthh:mm:ss[.mmm]")){
            String pattern = "(.*)-(.*)-(.*)[Tt](.*):(.*):(.*)";
            Pattern r = Pattern.compile(pattern);
            Matcher m = r.matcher(input[0]);
            String year="-1";
            String month ="-1";
            String date = "-1";
            String hour = "-1";
            String minutes = "-1";
            String seconds = "-1";
            String millisecs = "-1";
            if (m.find( )) {
                year = m.group(1);
                month = m.group(2);
                date = m.group(3);
                hour = m.group(4);
                minutes = m.group(5);
                seconds =  m.group(6);
                if(seconds.contains(".")){
                    pattern = "(.*)\\.(.*)";
                    r = Pattern.compile(pattern);
                    m = r.matcher(seconds);
                    if(m.find()) {
                        seconds = m.group(1);
                        millisecs = m.group(2);
                    }
                }
                //Two digits are always prepended with 19 in sql timestamp
                if(year.length()==2){
                    year = "19"+year;
                }

                if(millisecs.equals("-1"))
                    return_string = year+"-"+month+"-"+date+" "+hour+":"+minutes+":"+seconds;
                else
                    return_string = year+"-"+month+"-"+date+" "+hour+":"+minutes+":"+seconds+"."+millisecs;

            }
        }

        //YYYYMMDD[ hh:mm:ss[.mmm]]
        if(input[1].equals("YYYYMMDD[ hh:mm:ss[.mmm]]")){
            String pattern = "(\\d+)\\s?(.*)";
            Pattern r = Pattern.compile(pattern);
            Matcher m = r.matcher(input[0]);
            String y_m_d="-1";
            String h_m_s="-1";
            String year="-1";
            String month ="-1";
            String date = "-1";
            String hour = "00";
            String minutes = "00";
            String seconds = "00";
            String millisecs = "-1";
            if (m.find( )) {
                y_m_d = m.group(1);
                if(y_m_d.length()<8)
                    return return_string;
                h_m_s = m.group(2);
                year = y_m_d.substring(0,4);
                month = y_m_d.substring(4,6);
                date = y_m_d.substring(6,8);
                if(h_m_s.trim()!=""){
                    pattern = "(.*):(.*):(.*)";
                    r = Pattern.compile(pattern);
                    m = r.matcher(h_m_s);
                    if(m.find()){
                        hour = m.group(1);
                        minutes = m.group(2);
                        seconds = m.group(3);
                        if(seconds.contains(".")){
                            pattern = "(.*)\\.(.*)";
                            r = Pattern.compile(pattern);
                            m = r.matcher(seconds);
                            if(m.find()) {
                                seconds = m.group(1);
                                millisecs = m.group(2);
                            }
                        }
                    }
                }

                if(millisecs.equals("-1"))
                    return_string = year+"-"+month+"-"+date+" "+hour+":"+minutes+":"+seconds;
                else
                    return_string = year+"-"+month+"-"+date+" "+hour+":"+minutes+":"+seconds+"."+millisecs;

            }
        }
        return return_string;
    }
}
