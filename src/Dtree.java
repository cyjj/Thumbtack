import java.util.*;
import java.io.*;
/*
 *it is a ID3 decision tree program,based on the format of given data file 
 *class node describe the node type in tree, while class ID3 do the reading file, building tree and testing work
 * final print result is: 1.decision tree follows the format of example. 2.accuracy of using original file and test file 
 */
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
	ArrayList<String[]> train_data = new ArrayList<String[]>();
	ArrayList<String[]> num_in_attr= new ArrayList<String[]>();
	int total_num = 0;
	int dim;
	int[] dinc_val;
	String tag;
	public void treader(String txt)
	{//read in the training files
		String txtline = "";
	try
	{
		File filename = new File(txt);
		InputStreamReader reader = new InputStreamReader(new FileInputStream(filename));
		BufferedReader br = new BufferedReader(reader);
		
		String str = "";
		br.readLine();
		while( (str = br.readLine() )!= null)
		{
			total_num++;     //get total number of instances
			txtline = str;
			train_data.add(txtline.split("	"));
		}
		br.close();
	}catch (IOException e)
	   {
		e.printStackTrace();
	   }
	dim = train_data.get(0).length -1;	//find how many different attributes in the file
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
    } 
	public void shuffle()
	{//find the unique values in each attribute
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
	
	public double calculateEntropy(ArrayList<String[]> condition_instance)
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
	public ArrayList<String[]> getsubset(ArrayList<String[]> set,int attrnum, String value)
	{//get the subtree of next level
		Iterator<String[]> iter = set.iterator();
		String[] a = new String[dim];
		ArrayList<String[]> subset = new ArrayList<String[]>();
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
	public String findtag(ArrayList<String[]> set)
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
	public node split(ArrayList<String[]> m,int n,Boolean[] used_a)
	{//split the current, to gain the greatest ig, i just need to minimize the P*H of the next level
		int count=0;
		double bestentropy = 100.0;
		int selected_attr=0;
		Boolean[] used = new Boolean[used_a.length];
		for(int i=0; i< used_a.length;i++)
		{
			used=Arrays.copyOf(used_a, used_a.length);
		}
		ArrayList<String[]> b = new ArrayList<String[]>();			 	
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
					
					for(int a=0;a<n;a++)
					{
						System.out.print(" ");
					}
					System.out.println("attr"+i+" = "+num_in_attr.get(i)[k]+" : "+leafnode.label[k]);
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
				ArrayList<String[]> sub = new ArrayList<String[]>();
				
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
				for(int a=0;a<n;a++)
				{
					System.out.print(" ");
				}
				System.out.println("attr"+selected_attr+" = "+num_in_attr.get(selected_attr)[k]+" : "+datapoint.label[k]);
			}
			return datapoint;
		}
		else
		{//we have omitted all situation of leafnode, so this node is an internal node
			for(int k =0;k<dinc_val[selected_attr];k++)
			{
				ArrayList<String[]> sub = new ArrayList<String[]>();
				sub = getsubset(m,selected_attr,num_in_attr.get(selected_attr)[k]);
				
				for(int j=0;j<n;j++)
				{
					System.out.print(" ");
				}
				System.out.println("attr"+selected_attr+" = "+num_in_attr.get(selected_attr)[k]+" : ");
				
				datapoint.children[k] = split(sub,n+1,used);
			}
			return datapoint;
		}
		
	}
	
	public void getaccurancy(String name, node root) throws CloneNotSupportedException
	{
		ArrayList<String[]> test_data = new ArrayList<String[]>();
		int total = 0;
		int count=0;
		
		String txtline = "";
		try
		{
			File filename = new File(name);
			InputStreamReader reader = new InputStreamReader(new FileInputStream(filename));
			BufferedReader br = new BufferedReader(reader);
			
			String str = "";
			br.readLine();
			while( (str = br.readLine() )!= null)
			{
				total++;
				txtline = str;
				test_data.add(txtline.split("	"));
			}
			br.close();
		}catch (IOException e)
		   {
			e.printStackTrace();
		   }
		Iterator<String[]> iter = test_data.iterator();
		for(;iter.hasNext();)
		{
			String[] b =iter.next();
			String a=find(root,b);
			
			if(a.equals(b[dim]))
			{
				count++;
			}
		}
		double accurancy = (double) count/test_data.size();
		System.out.println(accurancy);
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


public class Dtree {

	public static void main(String[] args) throws CloneNotSupportedException {
		// TODO Auto-generated method stub
	
		ID3 D = new ID3();
		D.treader(args[0]);
		D.shuffle();
		Boolean[] used_attr = new Boolean[D.dim];
		for(int i =0; i<used_attr.length;i++)
		{
			used_attr[i]=false;
		}
		node root = D.split(D.train_data, 0, used_attr);
		System.out.print("accurancy of using train data file:");
		D.getaccurancy(args[0],root);
		System.out.print("accurancy of using test data file:");
		D.getaccurancy(args[1],root);

	}

}
