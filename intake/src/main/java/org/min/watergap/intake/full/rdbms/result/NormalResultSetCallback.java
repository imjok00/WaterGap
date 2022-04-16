package org.min.watergap.intake.full.rdbms.result;

import org.min.watergap.common.annotation.ResultSetMapping;
import org.min.watergap.piping.translator.impl.BasePipingData;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 通过注解的方式获取ResultSet映射
 *
 * @Create by metaX.h on 2021/11/21 22:14
 */
public class NormalResultSetCallback<T extends BasePipingData> implements ResultSetCallback {

    private List<T> baseStructs = new ArrayList<>();

    private Class instanceClass;

    public NormalResultSetCallback(Class instanceClass) {
        this.instanceClass = instanceClass;
    }

    @Override
    public void callBack(ResultSet resultSet) throws SQLException {
        try {
            while (resultSet.next()) {
                T t = (T) instanceClass.newInstance();
                for (Field field : instanceClass.getDeclaredFields()) {
                    if(field.isAnnotationPresent(ResultSetMapping.class)) {
                        ResultSetMapping resultSetMapping = field.getAnnotation(ResultSetMapping.class);
                        String columnName = resultSetMapping.value();
                        Object columnValue = resultSet.getObject(columnName);
//                        setColumnValue(field, columnValue, t);
                    }
                }
                baseStructs.add(t);
            }
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

//    private void setColumnValue(Field field, Object value, Object obj) throws IntrospectionException, InvocationTargetException, IllegalAccessException {
//        PropertyDescriptor pd = new PropertyDescriptor(field.getName(), Column.class);
//        Method wM = pd.getWriteMethod();
//        if (value != null) {
//            wM.invoke(obj, value);
//        }
//    }

    public List<T> getBaseStructs() {
        return baseStructs;
    }
}
