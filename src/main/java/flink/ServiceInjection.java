package flink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import flink.functions.FlinkService;
import stream.Processor;
import stream.runtime.DependencyInjection;
import stream.runtime.setup.factory.ProcessorFactory;

/**
 * Inject service into a processor using any service.
 *
 * @author alexey
 */
public class ServiceInjection implements ProcessorFactory.ProcessorCreationHandler {

    static Logger log = LoggerFactory.getLogger(ServiceInjection.class);

    /**
     * List of FlinkService used as wrapper for real service implementations.
     */
    private final List<FlinkService> flinkServices;

    public ServiceInjection(List<FlinkService> flinkServices) {
        this.flinkServices = flinkServices;
    }

    @Override
    public void processorCreated(Processor p, Element from) throws Exception {
        // save name of all properties to be set for possibly several services
        // names can be field names or service names set through XML configuration
        List<String> props = new ArrayList<>(0);

        // collect all declared field (even from superclasses)
        List<Field> fields = getDeclaredFields(p);

        // iterate through declared fields and search for a service-field.
        for (Field field : fields) {

            if (DependencyInjection.isServiceImplementation(field.getType())) {
                log.info("Checking service-field {}", field.getName());

                String prop = field.getName();
                stream.annotations.Service sa = field.getAnnotation(stream.annotations.Service.class);

                // if annotation contains 'name' then use this name instead of fiel name
                // (service can be named through XML configuration
                if (sa != null && !sa.name().isEmpty()) {
                    prop = sa.name();
                }
                props.add(prop);

                log.info("Service field '{}' relates to property '{}' for processor {}",
                        field.getName(), prop, p);
//                if (prop.equals(property)) {
//                    Class<?> valueType;
//
//                    //TODO handle the case of an array!? when does it happen?
////                    if (field.getType().isArray()) {
////                        valueType = field.getType().getComponentType();
////                        if (valueType.isAssignableFrom(Service.class)) {
////                            boolean orig = field.isAccessible();
////                            field.setAccessible(true);
////                            field.set(p, );
////                            field.setAccessible(orig);
////                        } else {
////                            throw new Exception("Array type mis-match! Field '" + field.getName() + "' of type "
////                                    + field.getType().getComponentType() + "[] is not assignable from "
////                                    + resolvedRefs.getClass().getComponentType() + "[]!");
////                        }
////
////                    } else {
//                    valueType = field.getType();
//                    //TODO resolvedRefs are Services!
//                    FlinkService flinkService = getFlinkService(prop);
//                    if (flinkService != null) {
//                        if (valueType.isAssignableFrom(Service.class)) {
//                                boolean orig = field.isAccessible();
//                                field.setAccessible(true);
//                                field.set(p, flinkService.getService());
//                                field.setAccessible(orig);
//                        } else {
//                            throw new Exception("Field '" + field.getName()
//                                    + "' is not assignable with object of type "
//                                    + flinkService.getService().getClass());
//                        }
//                    } else {
//                        log.error("FlinkService with name {} were not found.", prop);
//                    }
//                    }

//                }

            }
        }


        for (String property : props) {
            for (Method m : p.getClass().getMethods()) {
                String name = "set" + property.toLowerCase();
                if (m.getName().toLowerCase().equalsIgnoreCase(name)
                        && m.getParameterTypes().length == 1) {

                    Class<?> type = m.getParameterTypes()[0];
                    if (type.isArray()) {
                        //TODO handle array init for services
                        log.error("Trying to handle array initialization for a service, but it " +
                                "is not implemented yet. Please, contact developer.");

//                        Object values = Array.newInstance(type.getComponentType(), resolvedRefs.length);
//                        for (int i = 0; i < Array.getLength(values); i++) {
//                            Array.set(values, i, (resolvedRefs[i]));
//                        }
//                        log.debug("Injecting   '{}'.{}   <-- " + values, p, property);
//                        log.debug("Calling method  '{}'", m);
//                        m.invoke(p, values);

                    } else {
                        FlinkService flinkService = getFlinkService(property);
                        if (flinkService != null) {
                            log.debug("Injecting   '{}'.{}   <-- " + flinkService, p, property);
                            log.debug("Calling method  '{}' with arg '{}'", m, flinkService);
                            m.invoke(p, flinkService.getService());
                        } else {
                            log.error("FlinkService with name {} were not found.", property);
                        }
                    }
                }
            }
        }
    }

    /**
     * Collect declared field from current class and its superclasses.
     *
     * @param p processor with declared fields
     * @return list of found declared fields
     */
    private List<Field> getDeclaredFields(Processor p) {
        //TODO what if several superclasses?
        Field[] declaredFields = p.getClass().getDeclaredFields();
        List<Field> fields = new ArrayList<>(0);
        fields.addAll(Arrays.asList(declaredFields));
        Class<?> serv = p.getClass();
        while (serv.getSuperclass() != Object.class) {
            Class<?> superclass = serv.getSuperclass();
            Field[] declaredFields1 = superclass.getDeclaredFields();
            List<Field> fields1 = Arrays.asList(declaredFields1);
            fields.addAll(fields1);
            serv = superclass;
        }
        return fields;
    }

    /**
     * Iterate through the list of all services and find a service with the given name.
     *
     * @param name service name
     * @return FlinkService
     */
    private FlinkService getFlinkService(String name) {
        for (FlinkService service : flinkServices) {
            if (service.getServiceName().equals(name)) {
                return service;
            }
        }
        return null;
    }
}
