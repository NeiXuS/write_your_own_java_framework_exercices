package com.github.forax.framework.mapper;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class JSONWriter {

    private interface JSONFunction{
        String apply(JSONWriter writer, Object instance);
    }

    private static final class PropertyDescriptorClassValue extends ClassValue<List<JSONFunction>>{
        @Override
        protected List<JSONFunction> computeValue(Class<?> type){
            var beanInfo = Utils.beanInfo(type);
            return Arrays.stream(beanInfo.getPropertyDescriptors())
                    .filter(property -> !property.getName().equals("class"))
                    .<JSONFunction>map(propertyDescriptor -> {
                        var name = propertyDescriptor.getName();
                        var getter = propertyDescriptor.getReadMethod();
                        var prefix = "\"" + name + "\": ";
                        return (JSONWriter writer, Object o) -> prefix + writer.toJSON(Utils.invokeMethod(o, getter));
                    })
                    .toList();
        }

    }
    //private record Property(String prefix, Method getter){}

    private static final PropertyDescriptorClassValue CLASS_VALUE = new PropertyDescriptorClassValue();

    private String toJSONObject(Object o) {
        var properties = CLASS_VALUE.get(o.getClass());
        return properties.stream()
                .map(jsonFunction -> jsonFunction.apply(JSONWriter.this, o))
                .collect(Collectors.joining(", ", "{", "}"));
    }

    public String toJSON(Object o) {
        return switch(o){
            case Boolean b -> b.toString();
            case Integer i -> i.toString();
            case Double d -> d.toString();
            case String s -> "\"" + s + "\"";
            case null -> "null";
            case Object _ -> toJSONObject(o);
        };
    }


}
