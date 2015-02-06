package my.com.example.pratice;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.URI;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Map extends Mapper<Text, Text, Text, Text> {
	
	 String s;
	 String[] inputWords;
	 String neg[] = {"abandon","abuse","abusi","ache","aching","advers","afraid","aggravat","aggress","agitat",
		"agoniz","agony","alarm","alone","anger","angr","anguish","annoy","antagoni","anxi",
		"apath","appall","apprehens","argh","argu","arrogan","asham","assault","asshole","attack",
		"aversi","awful","awkward","bad","bashful","bastard","beaten","bitch","bitter","blam",
		"bore","boring","bother","broke","brutal","bum","bumhole","burden","careless","cheat",
		"complain","confront","confus","contempt","contradic","crap","crappy","craz","cried","cries",
		"critical","critici","crude","cruel","crushed","cry","crye","crying","cunt","cynic",
		"damag","damn","danger","daze","decay","defeat","defect","defenc","degrad","depress",
		"depriv","despair","desperat","despis","destroy","destruct","devastat","devil","difficult","disappoint",
		"disaster","discomfort","discourag","disgust","dishearten","disillusion","dislike","disliked","dislikes","disliking",
		"dismay","dissatisf","distract","distraught","distress","distrust","disturb","doom","dork","doubt",
		"dread","dull","dumb","dump","dwell","egotis","embarrass","emotional","empt","enemie",
		"enemy","enrag","envie","envious","envy","evil","exhaust","fail","fake","fatal",
		"fatigu","fault","fear","feared","fearful","fearing","fears","feroc","feud","fiery",
		"fight","fired","flame","flamed","flunk","foe","fool","forbid","fought","frantic",
		"freak","fright","frustrat","fuck","fucked","fucker","fuckin","fucks","fud","fume",
		"fuming","furious","fury","gay","geek","gloom","goddam","gossip","grave","greed",
		"grief","griev","grim","gross","grouch","grr","guilt","h8","hack","harass",
		"harm","harmed","harmful","harming","harms","hate","hated","hateful","hater","hates",
		"hating","hatred","heartbreak","heartbroke","heartless","hectic","hell","hellish","helpless","hesita",
		"homesick","hopeless","horr","hostil","humiliat","hungr","hurt","idiot","ignor","immoral",
		"impatien","impersonal","impolite","inadequa","indecis","ineffect","inferior","inhib","insecur","insincer",
		"insult","interrup","intimidat","irrational","irrita","isolat","jaded","jealous","jerk","jerked",
		"jerks","killed","killer","killing","lame","lazie","lazy","liabilit","liar","lied",
		"lies","lone","longing","lose","loser","loses","losing","loss","lost","lous",
		"low","luckless","ludicrous","lying","mad","maddening","madder","maddest","maniac","masochis",
		"melanchol","mess","messy","miser","miss","missed","misses","missing","mistak","mock",
		"mocked","mocker","mocking","mocks","molest","mooch","moodi","moody","moron","mourn",
		"murder","nag","nast","needy","neglect","nerd","nervous","neurotic","numb","nutter",
		"obnoxious","obsess","offence","offend","offens","outrag","overwhelm","pain","pained","painf",
		"paining","pains","panic","paranoi","pathetic","peculiar","perver","pessimis","petrif","pettie",
		"petty","phobi","piss","piti","pity","poison","poor","prejudic","pressur","prick",
		"problem","protest","protested","protesting","puk","punish","rage","raging","rancid","rape",
		"raping","rapist","rat","rebel","reek","regret","reject","reluctan","remorse","repress",
		"resent","resign","restless","revenge","ridicul","rigid","risk","rotten","rude","ruin",
		"sad","sadde","sadly","sadness","sarcas","savage","scare","scariest","scaring","scary",
		"sceptic","scream","screw","scum","scummy","selfish","serious","seriously","seriousness","severe",
		"shake","shaki","shaky","shame","shit","shock","shook","shy","sicken","sin",
		"sinister","sins","skeptic","slut","smother","smug","sneak","snob","sob","sobbed",
		"sobbing","sobs","solemn","sorrow","sorry","spam","spite","squirm","stammer","stank",
		"startl","steal","stench","stink","strain","strange","stress","struggl","stubborn","stunk",
		"stunned","stuns","stupid","stutter","submissive","suck","sucked","sucker","sucks","sucky",
		"suffer","suffered","sufferer","suffering","suffers","suk","suspicio","tantrum","tears","teas",
		"temper","tempers","tense","tensing","tension","terribl","terrified","terrifies","terrify","terrifying",
		"terror","thief","thieve","thirsty","threat","ticked","timid","tortur","tough","traged",
		"tragic","trauma","trembl","trick","trite","trivi","troubl","turmoil","ugh","ugl",
		"unattractive","uncertain","uncomfortabl","uncontrol","uneas","unfortunate","unfriendly","ungrateful","unhapp","unimportant",
		"unimpress","unkind","unlov","unpleasant","unprotected","unsavo","unsuccessful","unsure","unwelcom","upset",
		"uptight","useless","vain","vanity","vicious","victim","vile","villain","violat","violent",
		"vulnerab","vulture","war","warfare","warred","warring","wars","weak","weapon","weep",
		"weird","wept","whine","whining","whore","wickedn","wimp","witch","woe","worr",
		"worse","worst","worthless","wrong","wtf","yearn",
		"aren't","arent","can't","cannot","cant","couldn't","couldnt","don't","dont","isn't","isnt",
		"never","not","won't","wont","wouldn't","wouldnt"};

	 String pos[] = {"admir","ador","adventur","affection","agreeab","alol","amaz","amor","amus","aok",
		"appreciat","attachment","attract","award","awesome","baby","beaut","beloved","best","bff",
		"bg","bless","bonus","brave","brillian","calm","care","cared","carefree","cares",
		"caring","charm","cheer","cherish","chuckl","clever","comed","comfort","compassion","compliment",
		"confidence","confident","confidently","considerate","contented","contentment","cool","coolest","courag","cute",
		"cutie","daring","darlin","dear","delectabl","delicious","deligh","determina","determined","devot",
		"digni","divin","dynam","eager","ease","easie","easily","easiness","easing","easy",
		"ecsta","elegan","encourag","energ","engag","enjoy","enthus","excel","excit","fab",
		"fabulous","faith","fantastic","favor","favour","fearless","festiv","fiesta","fine","flawless",
		"flexib","flirt","fond","fondly","fondness","forgave","forgiv","fun","funn","genero",
		"gentle","gentler","gentlest","gently","giggl","glad","gladly","glamor","glamour","glori",
		"glory","gmbo","good","goodness","gorgeous","grace","graced","graceful","graces","graci",
		"grand","grande","gratef","grati","great","grin","grinn","grins","ha","haha",
		"handsom","happi","happy","harmless","harmon","heartfelt","heartwarm","heaven","hehe","hero",
		"hey","hilarious","hoho","homie","honest","honor","honour","hope","hoped","hopeful",
		"hopefully","hopefulness","hopes","hoping","hug","hugg","hugs","humor","humour","hurra",
		"importan","impress","improve","improving","incentive","innocen","inspir","intell","interest","invigor",
		"joke","joking","joll","joy","keen","kidding","kind","kindly","kindn","kiss",
		"laidback","laugh","like","likeab","liked","likes","liking","livel","lmao","lol",
		"lolol","love","loved","lovely","lover","loves","lovin","loving","loyal","luck",
		"lucked","lucki","lucks","lucky","luv","madly","magnific","mate","merit","merr",
		"muah","neat","nice","nurtur","ok","okay","okays","oks","omfg","omg",
		"openminded","openness","opportun","optimal","optimi","original","outgoing","painl","palatabl","paradise",
		"partie","party","passion","peace","perfect","play","played","playful","playing","plays",
		"pleasant","please","pleasing","pleasur","popular","positiv","prais","precious","prettie","pretty",
		"pride","privileg","prize","profit","promis","proud","radian","readiness","ready","reassur",
		"relax","relief","reliev","resolv","respect","revigor","reward","rich","rock","rofl",
		"romanc","romantic","safe","satisf","save","scrumptious","secur","sentimental","sexy","share",
		"shared","shares","sharing","silli","silly","sincer","smart","smil","sociab","soulmate",
		"special","splend","strength","strong","succeed","success","sunnier","sunniest","sunny","sunshin",
		"sup","super","superior","support","supported","supporter","supporting","supportive","supports","suprem",
		"sure","surpris","sweet","sweetheart","sweetie","sweetly","sweetness","sweets","talent","tehe",
		"tender","terrific","thank","thanked","thankf","thanks","thanx","thnx","thoughtful","thrill",
		"toleran","tranquil","treasur","treat","triumph","true","trueness","truer","truest","truly",
		"trust","truth","useful","valuabl","value","valued","values","valuing","vigor","vigour",
		"virtue","virtuo","vital","warm","wealth","welcom","well","wicked","win","winn",
		"wins","wisdom","wise","won","wonderf","worship","worthwhile","wow","x","xox",
		"xx","yay","yays","great"};
	 ArrayList<String> l1 = new ArrayList<String>();
	 ArrayList<String> l2 = new ArrayList<String>();
	 String positive;
	 static ArrayList<String> file1 = new ArrayList<String>();
	 ArrayList<String> posAndneg = new ArrayList<String>();
	 String negative;
	 String neutral;
	 int Flag = 0;
	 int  poscount = 0;
	 int negcount = 0;
	 double totalcount = 0.0;
	 double posProb = 0.0;
	 double negProb = 0.0;
	 double notposOrneg = 0.0;
	 double possiblypositive = 0.0;
	 double possiblynegative = 0.0;
	 double positiveOnly =0.0;
	 double negativeOnly = 0.0;
	 double posbayesCalculation =0.0;
	 double negbayesCalculation = 0.0;
	 ArrayList<String> a = new ArrayList<String>();
	 ArrayList<String> result = new ArrayList<String>();
	 ArrayList<String> value = new ArrayList<String>();
	 ArrayList<String> excludingWords = new ArrayList<String>();
     String[] words;
	      // private final  IntWritable one = new IntWritable(1);
	       Text sentiment = new Text();
	       Text value123 = new Text();
	       Path file_path;
	   	BufferedReader buffer_reader;
	      public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
	    	  excludingWords.add("this");
	  		excludingWords.add("is");
	  		excludingWords.add("a");
	  		excludingWords.add("for");
	  		excludingWords.add("in");
	  		excludingWords.add("or");
	  		excludingWords.add("should");
	  		excludingWords.add("can");
	  		excludingWords.add("an");
	  		excludingWords.add("but");
	  		excludingWords.add("and");
	  		excludingWords.add("movie");
	  		excludingWords.add(".");
	  		excludingWords.add(",");
	  		excludingWords.add(":");
	  		excludingWords.add("-");
	  		excludingWords.add("+");
	  		excludingWords.add("*");
	  		excludingWords.add(";");
	  		excludingWords.add("?");
	  		excludingWords.add("<");
	  		excludingWords.add(">");
	  		excludingWords.add("(");
	  		excludingWords.add(")");
	  		excludingWords.add("{");
	  		excludingWords.add("}");
	  		excludingWords.add("[");
	  		excludingWords.add("]");
	  		excludingWords.add("'");
	  		excludingWords.add("&");
	  		excludingWords.add("$");
	  		excludingWords.add("#");
	  		excludingWords.add("@");
	  		excludingWords.add("%");
	  		excludingWords.add("^");
	  		excludingWords.add("_");
	  		excludingWords.add("=");
	  		excludingWords.add("they");
	  		excludingWords.add("of");
	  		excludingWords.add("the");
	  		excludingWords.add("how");
	  		excludingWords.add("who");
	  		excludingWords.add("which");
	  		excludingWords.add("whom");
	  		excludingWords.add("why");
	  		excludingWords.add("his");
	  		excludingWords.add("her");
	  		excludingWords.add("it");
	  		excludingWords.add("he");
	  		excludingWords.add("she");
	  		excludingWords.add("that");
	  		excludingWords.add("these");
	  		excludingWords.add("which");
	  		excludingWords.add("make");
	  		excludingWords.add("even");
	  		excludingWords.add("since");
	  		excludingWords.add("to");
	  		excludingWords.add("too");
	  		excludingWords.add("what");
	  		excludingWords.add("with");
	  		excludingWords.add("off");
	  		excludingWords.add("it's");
	  		excludingWords.add("get");
	  		excludingWords.add("into");
	  		excludingWords.add("guys");
	  		excludingWords.add("see");
	  		excludingWords.add("him");
	  		excludingWords.add("has");
	  		excludingWords.add("then");
	  		// Create configuration for Map
			/*Configuration map_config = new Configuration();	 
		 	s = value.getText().toString();
	  		
           
				  inputWords = s.split("\\s+");
					
					for(int n=0;n<inputWords.length;n++)
				    {
				    	a.add(inputWords[n]);
				    }
					for(int e=0;e<a.size();e++)
				    {    String s1 = a.get(e);
				    excludingWords.add(s1);	
				    for(int k=0;k<neg.length;k++){
				        if(neg[k].equals (s1))
				        {
				        	excludingWords.remove(s1);	
				        }
				    }
				        for(int j=0;j<pos.length;j++){
						    if(pos[j].equals(s1))
						    {
						    	excludingWords.remove(s1);	
						    }
				        }
				    }
				    for(int i=0;i<a.size();i++)
				    {
				    	for(int j=0;j<pos.length;j++){
				    if(pos[j].equals(a.get(i)))
				    {
				    	
				    	if(a.get(i-1).equals("not")||(a.get(i-2).equals("not"))&& (a.get(i-1).equals("very")))
				    	{
				    	String p1 = a.get(i);
				    	a.remove(p1);
				    	negcount++;
				    	posAndneg.add(p1);
				    	
				    }
				    	else {
				    		String p1 = a.get(i);
					    	a.remove(p1);
					    	poscount++;
					    	posAndneg.add(p1);
				    	}
				    
				    	}
				    }
				    }
				    for(int b=0;b<a.size();b++)
				    { String s4 = a.get(b);
				    for(int k=0;k<neg.length;k++){
				        if(neg[k].equals (s4))
				        {
				        	String q = a.get(b);
				        	a.remove(q);
				        	negcount++;
				        	posAndneg.add(s4);
				        }  
				    }
				    }    	
				    totalcount = inputWords.length;
				    notposOrneg = totalcount - poscount-negcount;
				    possiblypositive = notposOrneg + poscount;
				    possiblynegative = notposOrneg + negcount;
				    posProb = possiblypositive/totalcount;
				    negProb = possiblynegative/totalcount;
				    positiveOnly = poscount/possiblypositive;
				    negativeOnly = negcount/possiblynegative;
				    posbayesCalculation = (positiveOnly*posProb)/((positiveOnly*posProb)+(negativeOnly*negProb));
				    negbayesCalculation = (negativeOnly*negProb)/((negativeOnly*negProb)+(positiveOnly*posProb));
				    if((posbayesCalculation)>(negbayesCalculation))
				    {
				    	
				    	
				    	String val1 = "";
				    	String respos = "Positive"+ " " +posbayesCalculation ;
				    	sentiment.set(respos);
				    	//value123.set(val1);
				    	 context.write(key, sentiment);
				    	 System.out.println("Positive Review, " + posbayesCalculation);
				    }	
				    else if ((posbayesCalculation)<(negbayesCalculation))
				    {
				    	
				    	
				    	String val2 = "";
				    	String res = "Negative" + " " + negbayesCalculation;
				    	sentiment.set(res);
				    	//value123.set(val2);
				    	context.write(key,sentiment);
				    	System.out.println("Negative Review, " + negbayesCalculation);
				    	
				}
				    else if ((posbayesCalculation) == (negbayesCalculation))
				    {
				    	
				    	String val = "0";
				    	String res1 = "Neutral" + " " + 0;
				    	sentiment.set(res1);
				    	//value123.set(val);
				    	
				    	context.write(key,sentiment);
				    	System.out.println("Neutral Review, 0");
				   	
				    }
					}
	          }
