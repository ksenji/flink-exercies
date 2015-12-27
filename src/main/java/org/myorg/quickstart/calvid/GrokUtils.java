package org.myorg.quickstart.calvid;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import oi.thekraken.grok.api.Grok;
import oi.thekraken.grok.api.exception.GrokException;

import org.apache.commons.lang3.StringUtils;

public class GrokUtils {

    public static Grok create(String grokPatternPath, String grokExpression) throws GrokException {
        if (StringUtils.isBlank(grokPatternPath)) {
            throw new GrokException("{grokPatternPath} should not be empty or null");
        }
        Grok g = new Grok();
        try {
            g.addPatternFromFile(grokPatternPath);
        } catch (GrokException e) {
            try (Reader r = new InputStreamReader(getResourceAsStream(grokPatternPath))) {
                g.addPatternFromReader(r);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        if (StringUtils.isNotBlank(grokExpression)) {
            g.compile(grokExpression);
        }
        return g;
    }

    public static InputStream getResourceAsStream(String name) {
        return Thread.currentThread().getContextClassLoader().getResourceAsStream(name);
    }
}
