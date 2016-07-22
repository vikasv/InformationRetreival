import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class newPageRank {
    
    public static Map<String, Double> initial = new HashMap<String, Double>();
    public static Map<String, Double> PR_new = new HashMap<String, Double>();
    public static HashMap<String, TreeSet<String>> outLinks = new LinkedHashMap<String, TreeSet<String>>();
    public static double lambda = 0.85;
    public double totalSink = 0;
    public static Double entropy = 0.0, oldperplexity = 0.0, newperplexity = 0.0;

    public class RankComparator implements Comparator<String> {

        Map<String, Double> map = new HashMap<String, Double>();

        public RankComparator(HashMap map) {
            this.map = map;
        }

        public RankComparator() {
            super();
        }

        @Override
        public int compare(String o1, String o2) {
            if (map.get(o1) >= map.get(o2)) {
                return -1;
            } else {
                return 1;
            }
        }

    }

    public class InLinkComparator extends RankComparator {

        Map<String, TreeSet<String>> graph = new LinkedHashMap<String, TreeSet<String>>();

        public InLinkComparator(HashMap graph) {
            super();
            this.graph = graph;
        }

        @Override
        public int compare(String o1, String o2) {
            if (graph.get(o1).size() >= graph.get(o2).size()) {
                return -1;
            } else {
                return 1;
            }
        }

    }

    public void calculatePageRankPerplexity(HashMap graph) {
        int loop = 0;
        String current_dir = System.getProperty("user.dir");

        try {

            double tempentropy = 0.0;
            for (Iterator iterator = graph.entrySet().iterator(); iterator.hasNext();) {
                
                Map.Entry next = (Map.Entry) iterator.next();
                
                initial.put(next.getKey().toString(), (double) (1 / (float) graph.size()));

                double temp2 = (double) (1 / (float) graph.size());

                tempentropy += temp2 * (Math.log(temp2) / Math.log(2));
            }

            newperplexity = Math.pow(2, (0 - tempentropy));

            FileWriter f = new FileWriter(current_dir + "//Perplexity.txt");

            BufferedWriter buf = new BufferedWriter(f);
            Set h = graph.entrySet();
            buf.write("List of perplexity values returned :\n");
            int change = 0;
            totalSink = 0;

            buf.write("Iteration " + change + " : " + newperplexity + "\n");
            change += 1;

            do {

                buf.flush();
                totalSink = 0;
                for (Iterator iterator = graph.entrySet().iterator(); iterator.hasNext();) {
                    Map.Entry next = (Map.Entry) iterator.next();
                    String checkKey = next.getKey().toString();
                    if (!outLinks.containsKey(checkKey)) {
                        totalSink = totalSink + initial.get(checkKey);
                    }
                }
                int n = graph.size();
                for (Iterator iterator = h.iterator(); iterator.hasNext();) { //for every element in map-graph

                    //Each page p in P
                    Map.Entry next = (Map.Entry) iterator.next();
                    TreeSet<String> array = (TreeSet) next.getValue();
                    String key = (String) next.getKey();

                    PR_new.put(key, ((lambda * totalSink) / n) + ((1 - lambda) / n));
                    
                    if (array != null) {
                        // Pages pointing to p
                        for (String str : array) {
                            if (PR_new.containsKey(key)) {
                                String node = str;
                                PR_new.put(key, PR_new.get(key) + ((initial.get(node) * lambda)
                                        / (outLinks.get(node).size())));
                            }

                        }
                    }
                    double pr = PR_new.get(key);
                    entropy += pr * (Math.log(pr) / Math.log(2));
                }
                oldperplexity = newperplexity;
                newperplexity = Math.pow(2, (0 - entropy));

                buf.write("Iteration " + change + " : " + newperplexity + "\n");
                buf.flush();

                initial.clear();
                initial = new HashMap(PR_new);
                entropy = 0.0;
                change++;
                PR_new.clear();
                System.out.println("-----Iteration " + change + " Completed------");
                if ((newperplexity - oldperplexity) < 1) {
                    loop += 1;
                } else {
                    loop = 0;
                }
            } while (loop < 4);

            RankComparator comp = new RankComparator((HashMap) initial);
            InLinkComparator comp2 = new InLinkComparator((HashMap) graph);
            TreeMap<String, Double> rankSortedMap = new TreeMap<String, Double>(comp);
            TreeMap<String, Double> linkSortedMap = new TreeMap<String, Double>(comp2);
            rankSortedMap.putAll(initial);
            linkSortedMap.putAll(graph);
            buf.flush();
            buf.close();

            try {

                FileWriter f2 = new FileWriter(current_dir + "//SortedPageRanks.txt");

                BufferedWriter buf2 = new BufferedWriter(f2);

                FileWriter f3 = new FileWriter(current_dir + "//SortedInLinks.txt");

                BufferedWriter buf3 = new BufferedWriter(f3);
                buf2.write("Top 500 Pages as sorted by Page Ranks :\n");
                int i = 0;
                for (Iterator iterator = rankSortedMap.entrySet().iterator(); iterator.hasNext() && i < 500;) {

                    Map.Entry next = (Map.Entry) iterator.next();

                    buf2.write(next.getKey() + " - " + next.getValue() + "\n");
                    i++;
                }
                i = 0;
                buf3.write("Top 500 Pages as sorted by number of InLinks :\n");
                for (Iterator iterator = linkSortedMap.entrySet().iterator(); iterator.hasNext() && i < 500;) {

                    Map.Entry next = (Map.Entry) iterator.next();

                    buf3.write(next.getKey() + " - " + ((TreeSet) next.getValue()).size() + "\n");
                    i++;
                }
                buf2.flush();
                buf2.close();
                buf3.flush();
                buf3.close();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }

            totalSink = 0;
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }

    public double pageRankDiff() {
        float diff = 0;

        for (Map.Entry next : PR_new.entrySet()) {
            diff += ((Double) next.getValue()) - initial.get(next.getKey());
        }
        return diff;
    }

    public static HashMap createGraphFromFile(String file) {

        System.out.println("Started creating graph...");
        outLinks.clear();

        HashMap<String, TreeSet<String>> graph = new LinkedHashMap<String, TreeSet<String>>();
        try {
            String current_dir = System.getProperty("user.dir");
            File f = new File(current_dir + "//" + file);
            BufferedReader buf = new BufferedReader(new FileReader(f));
            String line;
            String key = "";
            TreeSet<String> temp = new TreeSet<String>();
            TreeSet<String> inTemp = new TreeSet<String>();

            while ((line = buf.readLine()) != null) {

                String[] links = line.split(" ");
                int i = 0;

                for (String s : links) {
                	// 
                    if (i != 0) {
                        temp.add(s);
                        if (outLinks.containsKey(s)) {
                            inTemp = outLinks.get(s);
                            inTemp.add(key);
                            outLinks.put(s, new TreeSet<String>(inTemp));
                            inTemp.clear();
                        } else {
                            inTemp.add(key);
                            outLinks.put(s, new TreeSet<String>(inTemp));
                            inTemp.clear();
                        }
                    } else {
                        key = s;
                        i = 1;
                    }
                }

                if (graph.containsKey(key)) {
                    inTemp = graph.get(key);
                    inTemp.addAll(temp);
                    graph.put(key, new TreeSet<String>(inTemp));
                    inTemp.clear();
                } else {
                    graph.put(key, new TreeSet<String>(temp));
                }
                key = "";
                temp.clear();
            }

            int nodesSink = graph.size() - outLinks.size();

            System.out.println("Total Sink nodes - " + nodesSink);

            System.out.println("Data loaded...");

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return graph;
    }

 }
