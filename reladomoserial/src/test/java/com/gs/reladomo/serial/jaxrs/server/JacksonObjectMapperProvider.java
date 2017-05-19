package com.gs.reladomo.serial.jaxrs.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gs.reladomo.serial.jackson.JacksonReladomoModule;

import javax.ws.rs.ext.ContextResolver;

public class JacksonObjectMapperProvider implements ContextResolver<ObjectMapper>
{
    final ObjectMapper defaultObjectMapper;

    public JacksonObjectMapperProvider()
    {
        defaultObjectMapper = createDefaultMapper();
    }

    private static ObjectMapper createDefaultMapper()
    {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JacksonReladomoModule());
        return mapper;
    }

    @Override
    public ObjectMapper getContext(Class<?> type)
    {
        return defaultObjectMapper;
    }
}