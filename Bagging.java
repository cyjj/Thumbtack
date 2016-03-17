import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
/////////////////////////////////////////////////////////
//do bagging practise using decision tree and perceptron
//use old data given by professor, and in res.add(label);, loop all instances in each loop
//dateset: train.dat in decision tree category
/////////////////////////////////////////////////////////
class node implements Cloneable{
	int attr_num; //which attribute we use to divide the data point
 	node[] children;// children node
	String[] label;//if this node is a leaf node, it need a label
	public node(int dinc)
	{
		children = new node[dinc];
	    label =new String[dinc];
	}
	public void set(int attr)
	{
		this.attr_num = attr;
	}
	protected Object clone() throws CloneNotSupportedException
	{
		node newnode = (node) super.clone();
		newnode.children = (node[]) children.clone();
		newnode.label  = (String[]) label.clone();
		return newnode;
	}
	
}



 class ID3
{
	LinkedList<String[]> train_data = new LinkedList<String[]>();
	ArrayList<String[]> num_in_attr= new ArrayList<String[]>();
	ArrayList<String[]> test_data = new ArrayList<String[]>();
	int total_num = 0;
	int dim;
	int[] dinc_val;
	String tag;
	public ID3(LinkedList<String[]> train_data,ArrayList<String[]>test_data)
	{
		this.train_data= (LinkedList<String[]>) train_data.clone();
		this.test_data = (ArrayList<String[]>) test_data.clone();
		dim = train_data.get(0).length -1;	//find how many different attributes in the file
		total_num = train_data.size();
	}
	
	public void shuffle()
	{//find the unique values in each attribute
		
		int c1=0,c2=0; //in total scale which classification is more 
		for(int i=0;i<total_num;i++)
		{
			if(train_data.get(i)[dim]=="0")
			{
				c1++;
			}
			else
				c2++;
		}
		if(c1>c2)
		{
			tag = "0";
		}
		else
			tag = "1";
		String[] column = new String[total_num];
		dinc_val = new int[dim];
		for(int j =0 ; j<dim; j ++)
		{
			for(int i =0;i < total_num; i++)
			{
				column[i]=train_data.get(i)[j];
			}
			 Set<String> set = new HashSet<String>(Arrays.asList(column));
			 dinc_val[j] = set.size(); //number of different value in each attributes
			 num_in_attr.add(set.toArray(new String[set.size()])); //put each unique value in an array
		}
		
	 }
	
	public double calculateEntropy(LinkedList<String[]> condition_instance)
	{//calculate entropy
		double count1 = 0.0;
		double count2 = 0.0;
		double h = 0.0;
		int numdata = condition_instance.size();
		if(numdata == 0)
		{
			return -1.0;
		}
		for(int i=0;i < numdata; i ++)
		{
			if(condition_instance.get(i)[dim].equals("0"))
			{
				count1++;
			}
			else count2++;  
		}
		double prob1=count1/numdata;
		double prob2 = count2/numdata;
		if(count1==0|count2==0 )
			return 0.0;
		h = -(prob1 * (Math.log(prob1))/Math.log(2)+ prob2 * (Math.log(prob2))/Math.log(2));
		return h;
	}
	public LinkedList<String[]> getsubset(LinkedList<String[]> set,int attrnum, String value)
	{//get the subtree of next level
		Iterator<String[]> iter = set.iterator();
		String[] a = new String[dim];
		LinkedList<String[]> subset = new LinkedList<String[]>();
		for(int i=0;i<set.size();i++)
		{
		    a = set.get(i);
			if(a[attrnum].equals(value))
			{  
				subset.add(a);
			}
		}
	    return subset;
	}
	public String findtag(LinkedList<String[]> set)
	{//get the label for each branch, label is equal to most number instances's label
		int c1=0,c2=0;
		for(int i=0;i<set.size();i++)
		{
			if(set.get(i)[dim].equals("0"))
			{
				c1++;
			}
			else
				c2++;
		}
		if(c1>c2)
		{
			return"0";
		}
		if(c1<c2)
		{
			return "1";
		}
		if(c1==c2)
		{
			return tag;
		}
		return null;
			
	}
	public node split(LinkedList<String[]> m,int n,Boolean[] used_a)
	{//split the current, to gain the greatest ig, i just need to minimize the P*H of the next level
		int count=0;
		double bestentropy = 100.0;
		int selected_attr=0;
		Boolean[] used = new Boolean[used_a.length];
		for(int i=0; i< used_a.length;i++)
		{
			used=Arrays.copyOf(used_a, used_a.length);
		}
		LinkedList<String[]> b = new LinkedList<String[]>();			 	
		for(int i =0; i <dim; i++)
		{
			double avgentropy =0.0;
			if(used[i]==true)
			{//if this attr is used, omit it
				count++;
				continue;
			}
			for(int j =0;j<dinc_val[i];j++)
			{
				b=getsubset(m,i, num_in_attr.get(i)[j]);
				if(b.size()==0)
					continue;
				
				double subentropy = calculateEntropy(b);
				
				
				avgentropy+= subentropy*b.size();
				
			}
			if(avgentropy==(double)0)
			{// when we get a 0 entropy for subset, this means that the new node is a leaf node
				node leafnode = new node(dinc_val[i]);
				for(int k=0;k<dinc_val[i];k++)
				{
					leafnode.label[k] = findtag(getsubset(m,i,num_in_attr.get(i)[k]));
					
//					for(int a=0;a<n;a++)
//					{
//						System.out.print(" ");
//					}
//					System.out.println("attr"+i+" = "+num_in_attr.get(i)[k]+" : "+leafnode.label[k]);
					leafnode.set(i);
				}
				return leafnode;
			}
			avgentropy = avgentropy/ m.size();
			if(avgentropy < bestentropy)
			{
				bestentropy = avgentropy;
				selected_attr = i;
			}
		}
 		used[selected_attr] = true;
		node datapoint = new node(dinc_val[selected_attr]);
		datapoint.set(selected_attr);
		//now divide the data set using the selected attribute
		
		if(count==(dim-1))
		{//now we use all the attributes,datapoint is a leaf node
			for(int k =0;k<dinc_val[selected_attr];k++)
			{
				LinkedList<String[]> sub = new LinkedList<String[]>();
				
				sub = getsubset(m,selected_attr,num_in_attr.get(selected_attr)[k]);	
				double e= calculateEntropy(sub);
				if(e==(double)0)
				{//pure instances left 
					datapoint.label[k] = sub.get(0)[dim];
				}
				if(e==(double)-1)
				{//no instances left use the most frequent one in entire set
					datapoint.label[k]=tag;
				}
				if(e!=0.0&&e!=(double)-1)
				{//has impure instances left, use function
					datapoint.label[k] = findtag(sub);
				}

			}
			return datapoint;
		}
		else
		{//we have omitted all situation of leafnode, so this node is an internal node
			for(int k =0;k<dinc_val[selected_attr];k++)
			{
				LinkedList<String[]> sub = new LinkedList<String[]>();
				sub = getsubset(m,selected_attr,num_in_attr.get(selected_attr)[k]);
				
	
				datapoint.children[k] = split(sub,n+1,used);
			}
			return datapoint;
		}
		
	}
	
	public ArrayList<String> getaccurancy( node root) throws CloneNotSupportedException
	{
		
		ArrayList<String> res = new ArrayList<String>();

		Iterator<String[]> iter = test_data.iterator();
		for(;iter.hasNext();)
		{
			String[] b =iter.next();
			String a=find(root,b);
			res.add(a);
			
		}

		return res;

	}
 public String find(node root, String[] test) throws CloneNotSupportedException
 {//DFS way to find the label
	 
	 node n = (node)root.clone();
	 int k=0;
	 String a= new String();
	 while(k==0)
	 {
		 int attr = n.attr_num;
		 for(int i =0;i<dinc_val[attr];i++)
		 {
			 if(test[attr].equals(num_in_attr.get(attr)[i]))
			 {

				
				 if(n.children[i]!=null)
				 {
					 n = n.children[i];
				 }
				 else if(n.children[i]==null)
				 { 
					 k=1;
					 a = n.label[i];
				 }
				
			 }
		 }
		 
	 }
	 return a;

  }
}
 class nnet
 {
 	double [] w;
 	LinkedList<String[]> train_data = new LinkedList<String[]>();
 	int dim;
 	ArrayList<String[]> test_data = new ArrayList<String[]>();
 	
 	public nnet(LinkedList<String[]> train_data,ArrayList<String[]> test_data)
 	{
 		this.train_data = (LinkedList<String[]>) train_data.clone();
 		dim = train_data.get(0).length -1;	//find how many different attributes in the file which is equals to weight number
 	    w = new double[dim];
 	    this.test_data = (ArrayList<String[]>) test_data.clone();
 	}

 	public void update(double output,String[] in,double args)
 	{
 		double learning_rate = args;
 		double err = Double.parseDouble(in[dim]) - output;
 		for(int i =0;i<dim;i++)
 		{
 		    double input = Double.parseDouble(in[i]);
 			w[i] = w[i]+err* learning_rate*input*output*(1.0-output);
 		}
 	}
 	public void weightupdata(int iternum,double rate)
 	{
 		for(int j =0; j <iternum;j++)
 		{
 			Iterator<String[]> iter = train_data.iterator();
 			for(;iter.hasNext();)
 			{
 				String [] input = iter.next();
 				double output =0.0;
 				for(int i =0;i<dim;i++)
 				{
 					double in = Double.parseDouble(input[i]	);
 					output+= in*w[i];
 				}
 				 output = 1.0/(1.0+Math.exp(-output));
 				 
 				if(output != Double.parseDouble(input[dim]))
 				{
 					update(output,input,rate);
 				}

 			}
 		}

 	}
 	public ArrayList<String> test()
 	{
 		
 		ArrayList<String> res = new ArrayList<String>();

 		Iterator<String[]> iter = test_data.iterator();
 		double r = 0.0;
 		for(;iter.hasNext();)
 		{
 			double result = 0.0;
 			String [] test = iter.next();
 			String label =null;
 			for(int i = 0; i<dim ; i++)
 			{
 
 				result += w[i]*Double.parseDouble(test[i]);
 			}
 			
 		    r =  1.0/(1.0+Math.exp(-result));

 			if(r<0.5)
 			{
 				label = "0";
 				res.add(label);
 			}
 			else 
 			{
 				label = "1";
 				res.add(label);
 			}

 		}
 		return res;
 	}
 }
 

public class Bagging {
	private static LinkedList<String[]> input = new LinkedList<String[]>();
	private static LinkedList<LinkedList<String[]>> train_data = new LinkedList<LinkedList<String[]>>();
	private static ArrayList<String[]> test_data = new ArrayList<String[]>();
	public static void treader(String txt,List in)
	{//read in the files
		String txtline = "";
		int total_num = 0;
	try
	{
		File filename = new File(txt);
		InputStreamReader reader = new InputStreamReader(new FileInputStream(filename));
		BufferedReader br = new BufferedReader(reader);
		
		String str = "";
		br.readLine();
		//str = br.readLine();
		while( (str = br.readLine() )!= null)
		{
			total_num++;     //get total number of instances
			if(!str.trim().equals(""))
			{
				txtline = str;
			    in.add((txtline.split("	")));

			}
			
		}
		br.close();
	}catch (IOException e)
	   {
		e.printStackTrace();
	   }
     }
	public static void bootstrap(LinkedList<String[]> in, int iter_num)
	{//do bootstrapping bagging,randomly select instances with replacement from raw file
		
		Random random = new Random(System.currentTimeMillis());
		for(int i =0; i <iter_num;i++)
		{
			LinkedList<String[]> dataset = new LinkedList<String[]>();
			for(int j =0;j<in.size();j++)
			{
				int index = random.nextInt(in.size());
				dataset.add(in.get(index));
			}
			train_data.add(dataset);
			
		}
	}
	

	public static void main(String[] args) throws CloneNotSupportedException {
		// TODO Auto-generated method stub
		treader(args[0],input);//read in raw train data
		treader(args[1],test_data);//read in test data
		int baggings = Integer.parseInt(args[2]);//number of replicates
		bootstrap(input,baggings);//i prefer to use odd number to omit equal voting

		int t = Integer.parseInt(args[3]);
		ArrayList<ArrayList<String>> final_res = new ArrayList<ArrayList<String>>();
		ArrayList<String> vote = new ArrayList<String>();
		ArrayList<ID3> tree = new ArrayList<ID3>();
		ArrayList<nnet> perceptron = new ArrayList<nnet>(); 
		Iterator<LinkedList<String[]>> iter = train_data.iterator();
		int count = 1;
		if(t>baggings)
		{// number of one classifier should not out the range of bagging
			System.out.println("out of boundary, exit");
			System.exit(-1);
		}
		while(iter.hasNext())
		{//train the classifiers
			LinkedList<String[]> temp=iter.next();
			if(count<=t)
			{
				tree.add(new ID3(temp,test_data));
			}
			else
				perceptron.add(new nnet(temp,test_data));

			count++;
		}
		Iterator<ID3> iter_t = tree.iterator();
		Iterator<nnet> iter_n = perceptron.iterator();

		while(iter_t.hasNext())
		{//do test on decision trees
			ID3 D = iter_t.next();
			Boolean[] used_attr = new Boolean[D.dim];
			D.shuffle();
			for(int i =0; i<used_attr.length;i++)
			{
				used_attr[i]=false;
			}
			node root = D.split(D.train_data, 0, used_attr);
			final_res.add(D.getaccurancy(root));
			
			
		}
		while(iter_n.hasNext())
		{//test on perceptron, i choose to use 0.05 as learning rate and loop 1000 times
			nnet neural = iter_n.next();
			int iternum = Integer.parseInt("1000");
			double rate = Double.parseDouble("0.05");
			neural.weightupdata(iternum, rate);
			final_res.add(neural.test());
		}
		for(int i=0;i<final_res.get(0).size();i++)
		{//voting
			int c0=0,c1=0;
			Iterator<ArrayList<String>> iter_r = final_res.iterator();
		    while(iter_r.hasNext())
		    {
		    	if(iter_r.next().get(i).equals("0"))
		    	{
		    		c0++;
		    	}
		    	else 
		    		c1++;
		    }
		    if(c0>c1)
		    {// i use odd number of dataset, so that omit equal voting
		    	vote.add("0");	
		    }
		    else 
		    	vote.add("1");
			
		}
		Iterator<String[]> iter_test = test_data.iterator();
		Iterator<String> iter_v = vote.iterator();
		int counter=0;
		int index = test_data.get(0).length-1;
		while(iter_test.hasNext())
		{//calculate final accuracy
			if(iter_v.next().equals(iter_test.next()[index]))
			{
				counter++;
			}
		}
		float accuracy = (float) counter/test_data.size();
		System.out.println(accuracy);


	}

}
