package com.github.forax.framework.orm;

import jdk.jshell.execution.Util;
import org.h2.jdbcx.JdbcDataSource;

import javax.sql.DataSource;
import java.beans.*;
import java.io.Serial;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.GenericSignatureFormatError;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.sql.*;
import java.sql.Statement;
import java.time.Clock;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class ORM {
  private ORM() {
    throw new AssertionError();
  }

  @FunctionalInterface
  public interface TransactionBlock {
    void run() throws SQLException;
  }

  private static final Map<Class<?>, String> TYPE_MAPPING = Map.of(
      int.class, "INTEGER",
      Integer.class, "INTEGER",
      long.class, "BIGINT",
      Long.class, "BIGINT",
      String.class, "VARCHAR(255)"
  );

  private static Class<?> findBeanTypeFromRepository(Class<?> repositoryType) {
    var repositorySupertype = Arrays.stream(repositoryType.getGenericInterfaces())
        .flatMap(superInterface -> {
          if (superInterface instanceof ParameterizedType parameterizedType
              && parameterizedType.getRawType() == Repository.class) {
            return Stream.of(parameterizedType);
          }
          return null;
        })
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("invalid repository interface " + repositoryType.getName()));
    var typeArgument = repositorySupertype.getActualTypeArguments()[0];
    if (typeArgument instanceof Class<?> beanType) {
      return beanType;
    }
    throw new IllegalArgumentException("invalid type argument " + typeArgument + " for repository interface " + repositoryType.getName());
  }

  private static class UncheckedSQLException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 42L;

    private UncheckedSQLException(SQLException cause) {
      super(cause);
    }

    @Override
    public SQLException getCause() {
      return (SQLException) super.getCause();
    }
  }

  private static final ThreadLocal<Connection>  CONNECTION_THREAD_LOCAL = new ThreadLocal<>();
  public static void transaction(JdbcDataSource dataSource, TransactionBlock transaction) throws SQLException {
    Objects.requireNonNull(dataSource);
    Objects.requireNonNull(transaction);
    try(var connection = dataSource.getConnection()) {
      CONNECTION_THREAD_LOCAL.set(connection);
      connection.setAutoCommit(false);
      try{
        transaction.run();
      }catch (SQLException except){
        connection.rollback();
        throw except;
      }catch (UncheckedSQLException except){
        connection.rollback();
        throw except.getCause();
      } finally {
        CONNECTION_THREAD_LOCAL.remove();
      }
      connection.commit();

    }
  }

  static Connection currentConnection() {
    if (CONNECTION_THREAD_LOCAL.get() != null){
      return CONNECTION_THREAD_LOCAL.get();
    }
    else {
      throw new IllegalStateException("current connection called outside of transaction");
    }
  }

  static String findTableName(Class<?> beanClass){
    var annotation = beanClass.getAnnotation(Table.class);
    if(annotation != null) {
      return annotation.value();
    }
    else {
      return beanClass.getSimpleName().toUpperCase(Locale.ROOT);
    }
  }

  static String findColumnName(PropertyDescriptor property) {
    var getter = property.getReadMethod();
    if(getter != null){
      var annotation = getter.getAnnotation(Column.class);
      if(annotation != null) {
        return annotation.value();
      }
    }
    return property.getName().toUpperCase(Locale.ROOT);
  }
  static boolean isPresent(PropertyDescriptor property, Class<? extends Annotation> annotation){
    var getter = property.getReadMethod();
    return getter != null && getter.isAnnotationPresent(annotation);
  }

  public static void createTable(Class<?> beanClass) throws SQLException {
    Objects.requireNonNull(beanClass);
    var tableName = findTableName(beanClass);
    var connection = currentConnection();
    var beanInfos = Utils.beanInfo(beanClass);
    var columns = Arrays.stream(beanInfos.getPropertyDescriptors())
            .filter(pd -> !pd.getName().equals("class"))
            .map(pd -> {
              var column = findColumnName(pd);
              var typeName = TYPE_MAPPING.get(pd.getPropertyType());
              if(typeName == null){
                throw new IllegalStateException("invalid property type : " + pd);
              }
              var notNull = pd.getPropertyType().isPrimitive();
              var autoIncrement = isPresent(pd, GeneratedValue.class);
              var primaryKey = isPresent(pd, Id.class);
              return column + " " + typeName +
                      (notNull ? " NOT NULL" : "") +
                      (autoIncrement ? " AUTO_INCREMENT" : "") +
                      (primaryKey ? ",\n PRIMARY KEY (" + column + ")" : "");
            })
            .collect(Collectors.joining(",\n"));
    String update = "CREATE TABLE " + tableName + "(" + columns + ")";
    System.err.println(update);

    try(Statement statement = connection.createStatement()) {
      statement.executeUpdate(update);
    }
    connection.commit();
  }

  public static <R extends Repository<?, ?>> R createRepository(Class<R> repositoryType) throws UncheckedSQLException{
    Objects.requireNonNull(repositoryType);
    var beanType = findBeanTypeFromRepository(repositoryType);
    var tableName = findTableName(beanType);
    var constructor = Utils.defaultConstructor(beanType);
    var beanInfos = Utils.beanInfo(beanType);
    var properties = Arrays.stream(beanInfos.getPropertyDescriptors())
            .filter(property -> !property.getName().equals("class"))
            .toList();
    var propertiesMap = properties.stream()
            .collect(Collectors.toMap(PropertyDescriptor::getName, ORM::findColumnName));
    var propertyId = properties.stream().filter(pd -> isPresent(pd, Id.class)).findFirst().orElse(null);
    var columnNames = properties.stream().map(ORM::findColumnName).toList();
    return repositoryType.cast(Proxy.newProxyInstance(repositoryType.getClassLoader(),
            new Class<?>[]{repositoryType},
            (object, method, args) -> {
            try {
              return switch (method.getName()) {
                case String s when s.startsWith("findBy") -> {
                  var propertyName = Introspector.decapitalize(s.substring("findBy".length()));
                  var property = propertiesMap.get(propertyName);
                  if(property == null){
                    throw new IllegalStateException();
                  }
                  yield find("SELECT * FROM " + tableName + " WHERE " + property + " = ?", properties,
                          constructor, args[0]).stream().findFirst();
                }
                case "equals", "hashCode", "toString" -> throw new UnsupportedOperationException();
                case "findAll" -> find("SELECT * FROM " + tableName, properties, constructor);
                /*
                case "findById" -> {
                        if(propertyId == null){
                          throw new IllegalStateException();
                        }
                        yield find("SELECT * FROM " + tableName + " WHERE " + findColumnName(propertyId) + " = ?", properties,
                        constructor, args[0]).stream().findFirst();
                }
                */
                case "save" -> save(args[0], tableName, columnNames, properties, propertyId);
                default -> {
                  var annotation = method.getAnnotation(Query.class);
                  if(annotation != null) {
                    if(args != null){
                      yield find(annotation.value(), properties, constructor, args);
                    }
                    yield find(annotation.value(), properties, constructor);
                  }
                  throw new IllegalStateException();

                }
              };
            }catch (SQLException except){
              throw new UncheckedSQLException(except);
            }
            }));

  }
//  static PropertyDescriptor findProperty(BeanInfo beanInfo, String propertyName){
//    return Arrays.stream(beanInfo.getPropertyDescriptors()).filter(pd -> pd.getName().equals(propertyName)).findFirst().orElseThrow(IllegalStateException::new);
//  }

  static Object save(Object object, String tableName, List<String> columnNames, List<PropertyDescriptor> properties, PropertyDescriptor propertyId) throws SQLException{
    Objects.requireNonNull(object);
    Connection connection = currentConnection();
    var columns = String.join(", ", columnNames);
    var questionMarks = String.join(", ", Collections.nCopies(columnNames.size(), "?"));
    var update = "MERGE INTO " + tableName +"(" + columns + ") VALUES (" + questionMarks + ")";
    System.err.println(update);
    try(var statement = connection.prepareStatement(update, Statement.RETURN_GENERATED_KEYS)) {
      for(var i = 0; i< properties.size(); i++){
        var property = properties.get(i);
        var getter = property.getReadMethod();
        var value = Utils.invokeMethod(object, getter);
        statement.setObject(i+1, value);
      }
      statement.executeUpdate();
      if(propertyId!=null) {
        try (var resultSet = statement.getGeneratedKeys()) {
          if (resultSet.next()) {
            var id = resultSet.getObject(1);
            var setter = propertyId.getWriteMethod();
            Utils.invokeMethod(object, setter, id);
          }
        }
      }
    }
    return object;
  }

  static List<?> find(String query, List<PropertyDescriptor> properties, Constructor<?> constructor, Object... args) throws SQLException{
    var connection = currentConnection();
    try(PreparedStatement statement = connection.prepareStatement(query)) {
      for(var i = 0; i< args.length; i++){
        statement.setObject(i+1, args[i]);
      }
      try(ResultSet resultSet = statement.executeQuery()) {
        var list = new ArrayList<Object>();
        while(resultSet.next()) {
          var object = Utils.newInstance(constructor);
          for (int i = 0; i < properties.size(); i++) {
            var property = properties.get(i);
            var value = resultSet.getObject(i+1);
            var setter = property.getWriteMethod();
            if(setter!=null){
              Utils.invokeMethod(object, setter, value);
            }
            else{
              throw new IllegalStateException();
            }
          }
          list.add(object);
        }
        return list;
      }
    }

//    static Object findId()throw SQLException(){
//
//    }

//    public static Object toEntityClass(ResultSet resultSet, BeanInfo beanInfo, Constructor<?> constructor) {
//    }

  }
}
