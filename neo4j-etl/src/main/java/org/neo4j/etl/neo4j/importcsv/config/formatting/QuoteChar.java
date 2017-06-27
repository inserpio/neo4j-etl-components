package org.neo4j.etl.neo4j.importcsv.config.formatting;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.neo4j.etl.util.OperatingSystem;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.regex.Pattern;

import static java.lang.String.format;

public class QuoteChar {
    public static final QuoteChar DOUBLE_QUOTES = new QuoteChar("\"", OperatingSystem.isWindows() ? "\\\"" : "\"");
    public static final QuoteChar SINGLE_QUOTES = new QuoteChar("'", "'");
    public static final QuoteChar TICK_QUOTES = new QuoteChar("`", "`");

    private static final Pattern ESCAPE_CHAR_PATTERN = Pattern.compile("\\\\");
    private final String quote;
    private final String argValue;
    private final Pattern pattern;
    private final String escaped;
    public QuoteChar(String quote, String argValue) {
        this.quote = quote;
        this.argValue = argValue;
        this.pattern = Pattern.compile(quote, Pattern.LITERAL);
        this.escaped = format("%s%s", quote, quote);
    }

    public static QuoteChar fromJson(JsonNode root) {
        String quote = root.path("quote").textValue();
        String argValue = root.path("arg-value").textValue();

        return new QuoteChar(quote, argValue);
    }

    public String value() {
        return quote;
    }

    public String argValue() {
        return argValue;
    }

    @Deprecated
    public String enquote(String value) {
        StringWriter writer = new StringWriter();
        try {
            writeEnquoted(value, writer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return writer.toString();
    }

    public void writeEnquoted(String value, Writer writer) throws IOException {
        writer.write(quote);

        if (value.contains("\\")) {
            value = ESCAPE_CHAR_PATTERN.matcher(value).replaceAll("\\\\\\\\");
        }

        if (value.contains(quote)) {
            value = pattern.matcher(value).replaceAll(escaped);
        }

        writer.write(value);
        writer.write(quote);
    }

    public JsonNode toJson() {
        ObjectNode root = JsonNodeFactory.instance.objectNode();

        root.put("quote", quote);
        root.put("arg-value", argValue);

        return root;
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object o) {
        return EqualsBuilder.reflectionEquals(this, o);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }
}
