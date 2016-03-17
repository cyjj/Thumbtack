import java.util.*;
import java.util.Map.Entry;
import java.io.*;

/*
 * try to apply viterbi algorithm in java with given data set. unknown the name of 
 * each state, i name it with s1,s2 and so on. one input file contains all the parameters
 * we assume that one kind of parameter will be in one line. the program will read them into
 * memory one line by one line. so make sure the input file is in right format. another input file
 * gives out the observation sequence in each line.
 * 
 * finally, put the output states sequence in a arraylist then print out.
 */

class pro_path
{//probability and path pair in each time step
	float prob;
	String path;
	 pro_path( String path, float prob)
	{
		this.path = path;
		this.prob = prob;
	}
}


class V
{/*
  *all the probabilities are put in hashmap.
  * it act like two dimension array,but easier to query
  */
	
	Map<String,Float> ini_state = new HashMap<String, Float>();
	String [] output_alp;
	String [] state;
	Map<String,Map<String,Float>> trans = new HashMap<String,Map<String,Float>>();
	Map<String,Map<String,Float>> output_distri = new HashMap<String,Map<String,Float>>();
	ArrayList<String> path = new ArrayList<String>();
	ArrayList<ArrayList<String>> obser = new ArrayList<ArrayList<String>>();
	
	
	public void treader(String txt)
	{//read in basic parameters
		int states;
	    int output;

	try
	{
		File filename = new File(txt);
		InputStreamReader reader = new InputStreamReader(new FileInputStream(filename));
		BufferedReader br = new BufferedReader(reader);
		
		String str = "";
		str = br.readLine();
		//read first line get the number of state
		states = Integer.parseInt(str);
		//define the states name
		state = new String[states];
		for(int i =0;i<states;i++)
		{
			String inter = "s"+(i+1);
			state[i] = inter;
		}

		String[] S = br.readLine().split(" ");
		ArrayList<String> s = new ArrayList<String>();
		s = new ArrayList<String>(Arrays.asList(S));
		Iterator<String> iter1 = s.iterator();
		//next line, probability of initial states
		for(String ini : state)
		{
			ini_state.put(ini,Float.parseFloat(iter1.next()));
		}
		s.clear();
		
		S = br.readLine().split(" ");
		//get transition probability
		//i use hashmap to store the probability of each state
		
		s = new ArrayList<String>(Arrays.asList(S));
		Iterator<String> iter2 = s.iterator();
		int state_seq=0;
		while(iter2.hasNext())
		{
			Map<String,Float> s1 = new HashMap<String,Float>();
			for(int i =0;i<states;i++)
			{//get one row for one state
				s1.put(state[i], Float.parseFloat(iter2.next()));
			}
			trans.put(state[state_seq], s1);//put that row into map
			state_seq++;
		}

		s.clear();
		str = br.readLine();
		output = Integer.parseInt(str);//number of observation state
		str = br.readLine();//output alphabets
		output_alp = str.split(" ");
		
		S = br.readLine().split(" ");//last line output distribution
		s = new ArrayList<String>(Arrays.asList(S));
		Iterator<String> iter3 = s.iterator();
		int out_seq=0;
		while(iter3.hasNext())
		{
			Map<String,Float> s2 = new HashMap<String,Float>();
			for(int i =0;i<output;i++)
			{//get one row for one state
				s2.put(output_alp[i], Float.parseFloat(iter3.next()));
			}
			output_distri.put(state[out_seq], s2);//put that row into map
			out_seq++;
		}

	
		br.close();
	}catch (IOException e)
	   {
		e.printStackTrace();
	   }

    } 
	
	public void oreader(String txt)
	{//read in the output sequence
		try
		{
			File filename = new File(txt);
			InputStreamReader reader = new InputStreamReader(new FileInputStream(filename));
			BufferedReader br = new BufferedReader(reader);
			
			String str = "";
			//br.readLine();
			
			while( (str = br.readLine() )!= null)
			{
				ArrayList<String> o = new ArrayList<String>();
				o.clear();
				String []S = str.split(" ");
				for(String s : S )
				{
					o.add(s);
				}
				obser.add(o);
				
			}
			br.close();
		}catch (IOException e)
		   {
			 e.printStackTrace();
		   }

	}
	private static Map<String,Float> sort(Map<String,Float> unsorted)
	{//do sorting job of the map based on the values
		List<Entry<String,Float>> list = new LinkedList<Entry<String,Float>>(unsorted.entrySet());
		Collections.sort(list,new Comparator <Entry<String,Float>>()
		{
			public int compare(Entry<String,Float> o1, Entry<String,Float> o2)
			{

				return o2.getValue().compareTo(o1.getValue());
			}
		});
		
		//return a sorted map
		Map<String,Float> sorted = new LinkedHashMap<String,Float>();
		for(Entry<String,Float> entry :list)
		{
			sorted.put(entry.getKey(), entry.getValue());
		}
		return sorted;
	}
	
	public void backtracing (Map<String,ArrayList<pro_path>> b, String lastkey)
	{//find the former state with the given start point, which is the greatest last state

		String start = lastkey;
		while(true)
		{
			if(b.get(start).size() == 1)
			{//don't need initial state
				break;
			}
			
			String trace = b.get(start).get(0).path;

			path.add(0, trace);
			for(Entry<String, ArrayList<pro_path>> entry : b.entrySet())
			{
				entry.getValue().remove(0);
			}
			start = trace;
		}
	}
	
	public void forward_viterbi()
	{
		Iterator<ArrayList<String>> iter_obser = obser.iterator();
		Map<String,ArrayList<pro_path>> pre_prob = new LinkedHashMap<String,ArrayList<pro_path>>();
		LinkedHashMap<String,ArrayList<pro_path>> tmp = new LinkedHashMap<String,ArrayList<pro_path>>();
		while(iter_obser.hasNext())
		{//iterate for every output sequence
			float p;
			for(String states : state)
			{
				ArrayList<pro_path> s = new ArrayList<pro_path>();
				pre_prob.put(states,s);
			}
			ArrayList<String> seq =   iter_obser.next();//output sequence for each observation
			
			Iterator<String> Iter = seq.iterator();
			String ini_point = (String) Iter.next();
			for(String states : state)
			{//initiate the probability sequence
				p = ini_state.get(states)*(output_distri.get(states).get(ini_point));
				pro_path ini_prob = new pro_path(states,p);
				pre_prob.get(states).add(ini_prob);
				
			}
			Map<String,Float> unsorted = new TreeMap<String,Float>();
			Map<String, Float> sorted_desc;
			
			while(Iter.hasNext())
			{
				
				String label = (String) Iter.next();
				
				tmp.putAll(pre_prob);
				for(int i =0;i<state.length;i++)
				{//every state calculate the probabilities transit from other former states,then find the argmax among them
					
					for(Entry<String, ArrayList<pro_path>> entry : tmp.entrySet())
					{
						//transit from former states
						p = (entry.getValue().get(0).prob)*(trans.get(entry.getKey()).get(state[i]))*(output_distri.get(state[i]).get(label));
						unsorted.put(entry.getKey(), p);
					}
					sorted_desc = sort(unsorted);//the first value is the argmax
					//put the greatest value back to the pre_prob
					String myKey = (String) sorted_desc.keySet().toArray()[0];
					pro_path ele = new pro_path(myKey,sorted_desc.get(myKey)); 
					pre_prob.get(state[i]).add(0, ele);//done here with one current state, state[i]
					//clear the map used in this step
					unsorted.clear();
					sorted_desc.clear();
				}
				tmp.clear();
			}
			//in last state find which state with greatest at last
			for(Entry<String, ArrayList<pro_path>> entry : pre_prob.entrySet())
			{
				unsorted.put(entry.getKey(), entry.getValue().get(0).prob);
			}
			sorted_desc = sort(unsorted);
			//get the final state, and use it as entrance to back tracing
			String lastkey = (String) sorted_desc.keySet().toArray()[0];
			path.add(0,lastkey);
			float final_prob = sorted_desc.get(lastkey);
			backtracing(pre_prob,lastkey);
			System.out.print("for sequence: ");
			System.out.println(seq);
			System.out.print("the most possible path is:");
			System.out.println(path);
			pre_prob.clear();
			seq.clear();
			path.clear();
		}

		
		
	}
	
}

public class Viterbi {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		V v = new V();
		v.treader(args[0]);
		v.oreader(args[1]);
		v.forward_viterbi();

	}

}
