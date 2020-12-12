import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

/**
 * This method counts pagerank for each object mentioned in provided file
 */
public class CreatePageRank {
    //freebase.gz
    //freebase-head.gz
    //freebase.pokus.gz
    static String inputFile="freebase.pokus.gz"; //file from which program takes triplets
    static int maxLines=10000; //max number of lines I go through from input file (-1)
    static int numberOfIterations = 10; //how many times will program iterate through provided file to enumerate pagerank

    public static void cleanDirectory(){
        File dir = new File(".\\files\\output\\objects");
        for(File file: dir.listFiles())
            file.delete();
    }

    public static void processInputFile(String fileName) throws Exception{
        GZIPInputStream file = new GZIPInputStream(new FileInputStream(".\\files\\input\\"+fileName));
        BufferedReader br = new BufferedReader(new InputStreamReader(file));
        FileWriter output = new FileWriter(".\\files\\output\\output.txt");
        HashMap<String, String>map = new HashMap<String, String>();
        int counter =0; //pomocna premenna reprezentujuca pcoet prejdenych rriadkov pri testovani
        String sCurrentLine;
        while ((sCurrentLine = br.readLine()) != null) {
            counter++;
            if (counter > 0 && counter%10000000==0){ //priebezny vypis do suboru
                write(map, output);
                map.clear();
            }
            if (counter>maxLines)
                break;
            String subject="";
            String predicate="";
            String object="";
            int position = 0;
            if(sCurrentLine.length()==0)
                continue;

            //nacitanie udajov z riadku
            ArrayList<String> words = separateObjects(subject, predicate, object, sCurrentLine);
            subject = words.get(0);
            predicate = words.get(1);
            object = words.get(2);

            //vytiahnutie klucovych slov z predicate
            if(check(predicate)==0) continue;

            //vyhodenie textovych objektov
            if(check2(object)) continue;


            //idem skontrolovat ci uz mam subject v mape
            position=subject.indexOf("/ns/"); //sksuim vyhodit tie bez ns, chcem len objekty, nie odkazy na ine stranky
            String s;
            if (position==-1) {
                //s = subject.substring(1, subject.length() - 1);
                //sunjekt bez ns asi neexistuej ale pre istotu kontrolujem
                continue;
            }else {
                s = subject.substring(position + 4, subject.length() - 1);

            }
            String value;
            if((value=map.get(s))==null){
                map.put(s, "1");
            }else{//update objekt v mape
                int valueInt = Integer.parseInt(value);
                valueInt++;
                value = Integer.toString(valueInt);
                map.remove(s);
                map.put(s, value);
            }
            position=object.indexOf("/ns/");
            String o;
            if (position==-1) {
                //o = object.substring(1, object.length() - 1);
                continue;
            }else {
                o = object.substring(position + 4, object.length() - 1);
            }
            if((value=map.get(o))==null){
                map.put(o, "0");
            }else{//update objekt v mape
                continue;
            }

        }
        //treba este vypisat zvysok hashmapy o suboru
        write(map, output);
        file.close();
        output.close();
        br.close();
    }

    public static void sortFile() throws Exception{
        Process p = Runtime.getRuntime().exec(".\\util\\cmsort\\cmsort.exe .\\files\\output\\output.txt .\\files\\output\\sortedOutput.txt");
        p.waitFor();
        System.out.println("Waiting over, all sorted.");
    }

    public static void mergeFile()throws Exception{
        File sortedFile = new File(".\\files\\output\\sortedOutput.txt");
        File mergedFile = new File(".\\files\\output\\mergedOutput.txt");
        BufferedReader br = new BufferedReader(new FileReader(sortedFile));
        FileWriter output = new FileWriter(mergedFile);
        String name="", newName="", sCurrentLine="";;
        int value=0, newValue=0;
        int pomRiadok=0;
        while ((sCurrentLine = br.readLine()) != null) {
            if(pomRiadok==0){
                pomRiadok=1;
                name = sCurrentLine.substring(0, sCurrentLine.indexOf('\t'));
                value = Integer.parseInt(sCurrentLine.substring(sCurrentLine.indexOf('\t')+1));
            }else{
                newName = sCurrentLine.substring(0, sCurrentLine.indexOf('\t'));
                newValue = Integer.parseInt(sCurrentLine.substring(sCurrentLine.indexOf('\t')+1));
                //idem kuknut ci mergujem
                if (name.equals(newName)){//mergnem
                    value+=newValue;
                }else{//zapisem stare a prepisem nove
                    output.write(name+"\t"+value+",1,0\n");//nazov, pocet odkazov, pagerank, pom ci som uz upravoval pagerank
                    name=newName;
                    value=newValue;
                }
            }
            //zapisem do suboru
        }
        output.write(name+"\t"+value+",1,0\n");//nazov, pocet odkazov, pagerank, pom ci som uz upravoval pagerank
        output.close();
        br.close();
    }

    public static void divideFileIntoFiles() throws Exception{
        File mergedFile = new File(".\\files\\output\\mergedOutput.txt");
        BufferedReader br = new BufferedReader(new FileReader(mergedFile));
        int pocetVyskytov=0; //Just to be sure
        String line;
        File newFile;
        while ((line = br.readLine()) != null){
            String objectName = line.substring(0, line.indexOf('\t'));
            if(objectName.contains("/")){
                pocetVyskytov++;
                continue;
                //objectName=objectName.replace('/','_');
            }
            newFile = new File(".\\files\\output\\objects\\"+objectName+".txt");
            FileWriter output = new FileWriter(newFile);
            output.append(line);
            output.close();
        }
        br.close();
        System.out.println(pocetVyskytov);
    }

    public static void countPageRank(String fileName) throws Exception{
        for(int i=0; i<numberOfIterations; i++){
            System.out.println("iteration: "+i);
            GZIPInputStream file = new GZIPInputStream(new FileInputStream(".\\files\\input\\"+fileName));
            BufferedReader br = new BufferedReader(new InputStreamReader(file));
            int counter =0; //this will count number of lines. We dont have to look on lines, we never analysed in first place
            String sCurrentLine;
            while ((sCurrentLine = br.readLine()) != null){
                counter++;
                if (counter>maxLines) //if we stopped on this line in @processInputFile, we have to stop here as well
                    break;
                if(sCurrentLine.length()==0)
                    continue;
                //lets separate line into variables
                String subject="";
                String predicate="";
                String object="";
                ArrayList<String> words = separateObjects(subject, predicate, object, sCurrentLine);
                subject = words.get(0);
                predicate = words.get(1);
                object = words.get(2);

                //skip line if we skiped it in @processInputFile
                //vytiahnutie klucovych slov z predicate
                if(check(predicate)==0 || check2(object)) continue;


                /*now i need to open subject and object files.
                 *in subject i ll get number of links and pagerank
                 *in object i ll overwrite pagerank
                 */
                int position=subject.indexOf("/ns/");
                if (position==-1) {
                    continue;
                }else {
                    subject = getName(subject, position);
                }
                position=object.indexOf("/ns/");
                if (position==-1) {
                    continue;
                }else {
                    object = getName(object, position);
                }

                File subjectFile = new File(".\\files\\output\\objects\\"+subject+".txt");
                File objectFile = new File(".\\files\\output\\objects\\"+object+".txt");
                double subjectPageRank = getPageRank(subjectFile);
                int subjectLinks = getLinks(subjectFile);
                double objectPageRank = getPageRank(objectFile);
                int updated = getUpdated(objectFile);

                updatePageRank(objectFile, countPageRank(subjectPageRank, subjectLinks, objectPageRank, updated));
            }
            //reset pom variable in all files because we are starting new iteration
            resetFiles();
        }
    }

    public static void resetFiles() throws Exception{
        File dir = new File(".\\files\\output\\objects");
        for(File file: dir.listFiles())
            resetPom(file);
    }

    public static int check(String s){
        //if (s.indexOf("type.object.name")!=-1) return 0;
        Pattern pattern = Pattern.compile("name>|predicate>|date>|rate>|type\\.object\\.type|type\\.object\\.key|type\\.property\\.unique|type\\.property\\.schema|type\\.property\\.expected_type|type\\.property\\.expected_type|common\\.topic\\.description|\\.name|rdf-schema.+|rdf-syntax.+");
        Matcher matcher = pattern.matcher(s);
        boolean match = matcher.find();
        if(match) return 0;
        return 1;
    }

    public static boolean check2(String s){
        Pattern pattern = Pattern.compile("\".+[^>]");
        Matcher matcher = pattern.matcher(s);
        return matcher.find();
    }

    public static void write(HashMap<String, String> map, FileWriter output) throws Exception {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            output.write(entry.getKey()+"\t"+entry.getValue()+"\n");
        }
    }

    public static ArrayList<String> separateObjects(String subject, String predicate, String object, String sCurrentLine){
        int position=0, position2=0;
        position = sCurrentLine.indexOf("\t", position);
        subject = sCurrentLine.substring(0,position);
        position2 = sCurrentLine.indexOf("\t", position+1);
        predicate = sCurrentLine.substring(position+1,position2);
        position = sCurrentLine.indexOf("\t", position2+1);
        object = sCurrentLine.substring(position2+1,position);
        ArrayList<String> words = new ArrayList();
        words.add(subject);
        words.add(predicate);
        words.add(object);
        return words;
    }

    public static String getName(String s, int position){
        return s.substring(position + 4, s.length() - 1);
    }

    public static double getPageRank(File f) throws Exception{
        BufferedReader br = new BufferedReader(new FileReader(f));
        String line= br.readLine();
        String column = line.substring(line.indexOf('\t')+1);
        int location = column.indexOf(',');
        String pom = column.substring(location+1);
        location = pom.indexOf(',');
        pom=pom.substring(0,location);
        br.close();
        return Double.parseDouble(pom);
    }

    public static int getUpdated(File f) throws Exception{
        BufferedReader br = new BufferedReader(new FileReader(f));
        String line= br.readLine();
        String column = line.substring(line.indexOf('\t')+1);
        int location = column.indexOf(',');
        String pom = column.substring(location+1);
        location = pom.indexOf(',');
        pom=pom.substring(location+1);
        br.close();
        return Integer.parseInt(pom);
    }

    public static int getLinks(File f) throws Exception{
        BufferedReader br = new BufferedReader(new FileReader(f));
        String line= br.readLine();
        String column = line.substring(line.indexOf('\t')+1);
        int location = column.indexOf(',');
        String pom = column.substring(0,location);
        br.close();
        return Integer.parseInt(pom);
    }

    public static void updatePageRank(File f, double value) throws Exception{
        String finalLine = "";
        BufferedReader br = new BufferedReader(new FileReader(f));
        String line = br.readLine();
        br.close();
        String pom = line.substring(0,line.indexOf(','));
        finalLine+=pom+','+value+",1";
        FileWriter writer = new FileWriter(f);
        writer.write(finalLine);
        writer.close();
    }

    public static void resetPom(File f) throws Exception{
        String finalLine = "";
        BufferedReader br = new BufferedReader(new FileReader(f));
        String line = br.readLine();
        br.close();
        String pom = line.substring(0,line.indexOf(','));
        finalLine+=pom+',';
        pom = line.substring(line.indexOf(',')+1);
        finalLine+=pom.substring(0,pom.indexOf(','))+",0";
        FileWriter writer = new FileWriter(f);
        writer.write(finalLine);
        writer.close();
    }

    public static double countPageRank(double rankA, int links, double rankB, int updated){
        if(rankA==0)
            System.out.println("lopata");
        if (links == 0)
            links = 1;
        if (updated==1)
            return (rankB+(rankA/links));
        return (rankA/links);
    }

    public static void findMaxNodes(int numberOfHits) throws Exception {
        File dir = new File(".\\files\\output\\objects");
        BufferedReader br;
        String name, pom;
        Double value;
        ArrayList <String> names = new ArrayList<String>();
        ArrayList <Double> values = new ArrayList<Double>();

        for(File file: dir.listFiles()){

            br = new BufferedReader(new FileReader(file));
            String line = br.readLine();
            br.close();
            //System.out.println(line);
            name = line.substring(0,line.indexOf('\t'));
            pom = line.substring(line.indexOf(',')+1);
            value = Double.parseDouble(pom.substring(0,pom.indexOf(',')));
            if(names.size()<numberOfHits){
                names.add(name);
                values.add(value);
            }else{
                //i ll drom element with lowest rank
                int index = getIndexOfMin(values);
                values.remove(index);
                names.remove(index);
                names.add(name);
                values.add(value);
            }
        }
        //lets sort it a bit with BUBBLESORT
        for (int i=0; i<values.size(); i++){
            for (int j=0; j<values.size()-1; j++){
                if(values.get(j+1)>values.get(j)){
                    Collections.swap(values, j, j+1);
                    Collections.swap(names, j, j+1);
                }
            }
        }
        for (int i =0; i<names.size(); i++){
            System.out.println(names.get(i)+": "+values.get(i));
        }

    }

    public static int getIndexOfMin(ArrayList<Double> values){
        Double min=0.0;
        int MinId = 0;
        for (int i=0; i<values.size(); i++){
            if (i==0){
                min=values.get(i);
                MinId=i;
                continue;
            }
            if (values.get(i)<min) {
                min = values.get(i);
                MinId=i;
                continue;
            }
        }
        return MinId;
    }

    public static void main(String[] args) {
        try {
            //delete all object files from previous run of program
            cleanDirectory();
            System.out.println("Cleaned");
            //create new file with objects and number of connections
            processInputFile(inputFile);
            System.out.println("Processed");
            //sort File created by previous method
            sortFile();
            System.out.println("Sorted");
            //merge duplicate lines in sorted file
            mergeFile();
            System.out.println("Merged");
            //create new file for each line of merged file with its own name
            divideFileIntoFiles();
            System.out.println("Divided");
            //mam vytvorene subory
            countPageRank(inputFile);
            System.out.println("Counted");
            //find maximum
            findMaxNodes(10);
            //hladanie max hodnoty
            /*int maxvalue=0;
            List list = new ArrayList();
            for (Map.Entry<String, String> entry : map.entrySet()) {
                int Value = Integer.parseInt(entry.getValue());
                if (Value>maxvalue){
                    maxvalue=Value;
                    list.clear();
                    list.add(entry.getKey());
                }else if (Value==maxvalue){
                    list.add(entry.getKey());
                }
                // Do things with the list
            }
            if (maxvalue==0)
                System.out.println("Max value == 0");
            for (int i = 0; i<list.size(); i++){
                System.out.println(list.get(i) + " with value: "+maxvalue);
            }*/

        }catch (Exception e){
            System.out.println(e);
        }
    }
}
