package org.github.forax.framework.interceptor;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Array;
import java.util.*;
import java.util.stream.Stream;

public final class InterceptorRegistry {
  private final Map<Class<? extends Annotation>, List<AroundAdvice>> adviceList = new HashMap<>();
  private final Map<Class<? extends Annotation>, List<Interceptor>> interceptorsList = new HashMap<>();

//  public void addAroundAdvice(Class<? extends Annotation> annotationClass, AroundAdvice aroundAdvice){
//    Objects.requireNonNull(annotationClass);
//    Objects.requireNonNull(aroundAdvice);
//    adviceList.computeIfAbsent(annotationClass, _ -> new ArrayList<>()).add(aroundAdvice);
//  }

  public void addAroundAdvice(Class<? extends Annotation> annotationClass, AroundAdvice aroundAdvice){
      Objects.requireNonNull(annotationClass);
      Objects.requireNonNull(aroundAdvice);
      addInterceptor(annotationClass, (o, m, a, i) -> {
        aroundAdvice.before(o, m, a);
        Object result = null;
        try{
          result = i.proceed(o,m,a);
        }finally {
          aroundAdvice.after(o,m,a, result);
        }
        return result;
      });
    }
/*
  List<AroundAdvice> findAdvices(Method method){
    var annotations = method.getAnnotations();
    return Arrays.stream(annotations)
            .flatMap(annotation -> {
              var annType = annotation.annotationType();
              var advices = adviceList.getOrDefault(annType, List.of());
              return advices.stream();
            })
            .toList();
  }*/

//  public <T> T createProxy(Class<T> type, T implementation){
//    Objects.requireNonNull(type);
//    Objects.requireNonNull(implementation);
//    if(!type.isInterface()){
//      throw new IllegalArgumentException("type isnt interface : " + type.getName());
//    }
//    return type.cast(Proxy.newProxyInstance(type.getClassLoader(),
//        new Class<?>[] {type},
//        (_, method, args) -> {
//          var advices = findAdvices(method);
//          for(var advice : advices){
//            advice.before(implementation, method, args);
//          }
//          Object result = null;
//          try{
//            result  = Utils.invokeMethod(implementation, method, args);
//          }finally{
//            for(var advice : advices.reversed()){
//              advice.after(implementation, method, args, result);
//            }
//          }
//          return result;
//        }));
//  }

  public <T> T createProxy(Class<T> type, T implementation){
    Objects.requireNonNull(type);
    Objects.requireNonNull(implementation);
    if(!type.isInterface()){
      throw new IllegalArgumentException("type isnt interface : " + type.getName());
    }
    return type.cast(Proxy.newProxyInstance(type.getClassLoader(),
            new Class<?>[] {type},
            (_, method, args) -> {
              var invocation = getInvocation(findInterceptors(method));
              return invocation.proceed(implementation, method, args);
            }));
  }

  public void addInterceptor(Class<? extends Annotation> annotationClass, Interceptor interceptor){
    Objects.requireNonNull(annotationClass);
    Objects.requireNonNull(interceptor);
    interceptorsList.computeIfAbsent(annotationClass, _ -> new ArrayList<>()).add(interceptor);
  }

  static Invocation getInvocation(List<Interceptor> interceptors){
    Invocation invocation = Utils::invokeMethod;
    for(var interceptor : interceptors.reversed()){
      var copyInvocation = invocation;
      invocation = (o, m, a) -> interceptor.intercept(o,m,a, copyInvocation);
    }
    return invocation;
  }

  List<Interceptor> findInterceptors(Method method){
    var annotations = method.getAnnotations();
    return Arrays.stream(annotations)
            .flatMap(annotation -> {
              var annType = annotation.annotationType();
              var advices = interceptorsList.getOrDefault(annType, List.of());
              return advices.stream();
            })
            .toList();
  }
}
