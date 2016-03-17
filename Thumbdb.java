/*
Author: Yang

Date: 3/12

Topic: Thumbtack code challenge, simple database
 */

import java.io.*;
import java.lang.*;
import java.util.*;
class tranHis{
    /*one transaction history record which will be used by rollback
    given that the rollback operation will undo all operations in one block
    We dont need to record all activities, instead, we only have to record the original values that have been modified in this step
    In this simple database, it will save time and space.
     */
    private HashMap<String , Integer> backup; // use a hashmap to contain original value for one name
    public tranHis(){
        backup = new HashMap<>();
    }
    public HashMap<String , Integer> get(){
        return backup;
    }
}

class Database{
    private HashMap<String , Integer> name_value; // this dict store all name and value pairs, means one record
    private HashMap<Integer , Integer> value_count; // count the number of some value for "NUMEQUALTO"

    private Stack<tranHis> rollback; // record all transaction history for rollback.

    public Database(){
        name_value = new HashMap<>();
        value_count = new HashMap<>();
        rollback = new Stack<>();
    }

    public Stack<tranHis> getRollBack(){
        return rollback;
    }

    //Data Commands
    //set and unset
    //O(1)
    public void SET(String name , Integer value){
        //set new value, and maintain the count for that value
        Integer preValue = name_value.get(name);// get old value for this record
        tranHis cur = new tranHis();
        if(!rollback.isEmpty()){
            cur = rollback.peek();//get current transaction history
        }
        if(!cur.get().containsKey(name)){
            //if value of name have not be changed in this transaction, record it. else no record needed
            cur.get().put(name , preValue);
        }
        if(preValue != null){
            //if this record has been set one value, deduct one from its counter
            int preValueCount = value_count.get(preValue);
            value_count.put(preValue , -- preValueCount);
        }

        Integer curValueCount = value_count.get(value);
        //check the new value, if value is null, that means unset operation
        if(value != null){
            if(curValueCount != null){
                //current value has existed in database
                value_count.put(value , ++ curValueCount);
            }
            else{
                //this value is the only one in this database now
                value_count.put(value , new Integer(1));
            }
        }
        name_value.put(name , value);// update this record
    }

    //get
    //O(1)
    public Integer GET(String name){
        return name_value.get(name);
    }

    //NUMEQUALTO
    //O(1)
    public Integer NUMEQUALTO(Integer value){
        return value_count.get(value);
    }

    //rollback
    //O(N)
    public void ROLLBACK(){
        tranHis backup = rollback.pop();//get the rollback information for most recent transaction
        for(Map.Entry<String , Integer> entry : backup.get().entrySet()){
            String tmp_name = entry.getKey(); // name of record that need to be rollback
            Integer cur_value = name_value.get(tmp_name);// current value of this record
            Integer pre_value = entry.getValue();// value that need to rollback be
            if(cur_value != null){
                //not null, need to update the counter of current value
                value_count.put(cur_value , value_count.get(cur_value) - 1);
            }
            if(pre_value != null){
                //not null, update the counter of the previous value
                value_count.put(pre_value , value_count.get(pre_value) + 1);
            }
            name_value.put(tmp_name , pre_value); // update the record in global space
        }
    }

    //commit
    //O(1)
    public void COMMIT(){
        rollback.clear();
    }
}

public class Thumbdb {
    //for one database, I define all record be global accessible in this database, one record only contain current value
    public static void main(String[] args) {
        Database db = new Database();
        FileReader fileReader = null;
        String cmdLine = "";
        String name;// name of one record
        Integer value;// value which is queried

        try{

            if(args.length > 0){
                fileReader = new FileReader(args[0]);
            }
            BufferedReader br = null;
            if(fileReader == null){
                //no file name given, get command from keyboard
                InputStreamReader reader = new InputStreamReader(System.in);
                br = new BufferedReader(reader);
            }
            else{
                //get command from txt file
                br = new BufferedReader(fileReader);
            }
            while((cmdLine = br.readLine()) != null){
                String[] tokens = cmdLine.split("\\s+");
                String cmd = tokens[0]; // which operation next

                    switch (cmd){
                        case "GET":
                            name = tokens[1]; 
                            System.out.println(db.GET(name));
                            break;
                        case "NUMEQUALTO":
                            value = Integer.parseInt(tokens[1]); 
                            System.out.println(db.NUMEQUALTO(value));
                            break;
                        case "SET":
                            name = tokens[1];
                            value = Integer.parseInt(tokens[2]);
                            db.SET(name , value);
                            break;
                        case "UNSET":
                            name = tokens[1];
                            db.SET(name , null);
                            break;
                        case  "END":
                            return;
                        case  "BEGIN":
                            //create one new transaction, in our program, create a new transaction history in stack
                            //O(1)
                            //given the introduction, BEGIN command open a new transaction block
                            //without BEGIN command, no transaction history will be created.
                            tranHis newTranHis = new tranHis();
                            db.getRollBack().push(newTranHis);
                            break;
                        case "ROLLBACK":
                            if(db.getRollBack().isEmpty()){
                                //nothing to rollback
                                System.out.println("NO TRANSACTION");
                            }
                            else{
                                db.ROLLBACK();
                            }
                            break;
                        case "COMMIT":
                            if(db.getRollBack().isEmpty()){
                                //nothing to commit
                                System.out.println("NO TRANSACTION");
                            }
                            else{
                                db.COMMIT();
                            }
                            break;

                        default:
                            System.out.println("Invalid input command");
                    }

                }

            } catch (NumberFormatException e) {
                // invalid for SET function
                System.out.println("Invalid number format: " + cmdLine);
            }
            catch (ArrayIndexOutOfBoundsException e) {
                //missing operand for GET
                System.out.println("Possibly missing operand: " + cmdLine );
            }
            catch (IOException e){
                //error in reading file
                e.printStackTrace();
            }
    }
}
