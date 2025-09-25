package com.github.forax.framework.injector;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public final class InjectorRegistry {

    private final HashMap<Class<?>, Supplier<?>> suppliers = new HashMap<>();

    public <T> void registerInstance(Class<T> type, T instance){
        Objects.requireNonNull(type);
        Objects.requireNonNull(instance);
        if(suppliers.putIfAbsent(type, () -> instance) != null)
            throw new IllegalStateException("already in map " + type.getName());
    }


    public <T> T lookupInstance(Class<T> type){
        Objects.requireNonNull(type);
        var supplier = suppliers.get(type);
        if(Objects.isNull(supplier)){
            throw new IllegalStateException("key not found : " + type.getName());
        }
        return type.cast(supplier.get());
    }

    public <T> void registerProvider(Class<T> type, Supplier<T> supplier){
        Objects.requireNonNull(type);
        Objects.requireNonNull(supplier);
        if(suppliers.putIfAbsent(type, supplier) != null)
            throw new IllegalStateException("already in map " + type.getName());
    }

    static List<PropertyDescriptor> findInjectableProperties(Class<?> type){
        Objects.requireNonNull(type);
        var beanInfos = Utils.beanInfo(type);
        var properties = beanInfos.getPropertyDescriptors();
        return Arrays.stream(properties)
                .filter(property -> {
                var setter = property.getWriteMethod();
                return setter != null && setter.isAnnotationPresent(Inject.class);
            })
                .toList();
    }

    private  Constructor<?> findInjectableConstructor(Class<?> providerClass){
        Objects.requireNonNull(providerClass);
        var injectable = Arrays.stream(providerClass.getConstructors())
                .filter(c -> c.isAnnotationPresent(Inject.class))
                .toList();
        return switch(injectable.size()){
            case 0 -> Utils.defaultConstructor(providerClass);
            case 1 -> injectable.getFirst();
            default -> throw new IllegalStateException("Multiple injectable constructor" + providerClass.getName());
        };
    }

    public <T> void registerProviderClass(Class<T> providerClass){
        Objects.requireNonNull(providerClass);
        registerProviderClass(providerClass, providerClass);
    }

    public <T> void registerProviderClass(Class<T> type, Class<? extends T> providerClass){
        Objects.requireNonNull(type);
        Objects.requireNonNull(providerClass);
        var constructor = findInjectableConstructor(providerClass);
        var injectables = findInjectableProperties(type);
        var parameterTypes = constructor.getParameterTypes();
        registerProvider(type, () -> {
            var args = Arrays.stream(parameterTypes)
                    .map(this::lookupInstance)
                    .toArray();
            var object = Utils.newInstance(constructor, args);
            for(var injectable : injectables){
                var setter = injectable.getWriteMethod();
                var value = lookupInstance(injectable.getPropertyType());
                Utils.invokeMethod(object, setter, value);
            }
            return type.cast(object);
        });
    }
}