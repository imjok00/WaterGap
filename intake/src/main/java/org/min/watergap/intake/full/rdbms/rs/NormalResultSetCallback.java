package org.min.watergap.intake.full.rdbms.rs;

import org.min.watergap.common.annotation.ResultSetMapping;
import org.min.watergap.intake.full.rdbms.to.ColumnStruct;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 通过注解的方式获取ResultSet映射
 *
 * @Create by metaX.h on 2021/11/21 22:14
 */
public class NormalResultSetCallback implements ResultSetCallback {

    private List<ColumnStruct> baseStructs = new ArrayList<>();

    @Override
    public void callBack(ResultSet resultSet) throws SQLException {
        try {
            while (resultSet.next()) {
                ColumnStruct t = new ColumnStruct();
                for (Field field : ColumnStruct.class.getDeclaredFields()) {
                    if(field.isAnnotationPresent(ResultSetMapping.class)) {
                        ResultSetMapping resultSetMapping = field.getAnnotation(ResultSetMapping.class);
                        String columnName = resultSetMapping.value();
                        Object columnValue = resultSet.getObject(columnName);
                        setColumnValue(field, columnValue, t);
                    }
                }
                baseStructs.add(t);
            }
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    private void setColumnValue(Field field, Object value, Object obj) throws IntrospectionException, InvocationTargetException, IllegalAccessException {
        PropertyDescriptor pd = new PropertyDescriptor(field.getName(), ColumnStruct.class);
        Method wM = pd.getWriteMethod();
        if (value != null) {
            wM.invoke(obj, value);
        }
    }

    public List<ColumnStruct> getBaseStructs() {
        return baseStructs;
    }
}
