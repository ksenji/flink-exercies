package org.myorg.quickstart.calvid;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.regex.Pattern;

import oi.thekraken.grok.api.Grok;
import oi.thekraken.grok.api.Match;

public class GrokTest {

    public static void main(String[] args) throws Exception {
        
        Pattern p = Pattern.compile(".*?\\u007F(.*)");
        
        System.out.println("asdf :"+p.matcher("asdf").matches());
        System.out.println("asdf"+ 0x7F +"senji"+":"+p.matcher("asdf"+ (char)0x7f +"senji").matches());
        
        
        
        Grok g = GrokUtils.create("patterns", "%{CALVID}");
        try (BufferedReader br = new BufferedReader(new InputStreamReader(GrokUtils.getResourceAsStream("head")))) {
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
                Match m = g.match(line);
                if (m.getMatch().matches()) {
                    m.captures();

                    System.out.println(m.toMap());
                }
            }
        }
    }

}
