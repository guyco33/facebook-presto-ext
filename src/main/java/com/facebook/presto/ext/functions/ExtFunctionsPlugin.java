package com.facebook.presto.ext.functions;

import com.facebook.presto.spi.Plugin;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class ExtFunctionsPlugin implements Plugin {
    @Override
    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(ExtCSVFunctions.class)
                .add(ExtAuxFunctions.class)
                .add(ExtStatsFunctions.class)
                .add(ExtJsonFunctions.class)
                .build();
    }
}
