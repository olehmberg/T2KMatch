/** 
 *
 * Copyright (C) 2015 Data and Web Science Group, University of Mannheim, Germany (code@dwslab.de)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package de.uni_mannheim.informatik.dws.t2k.match.data;

import java.lang.reflect.InvocationHandler;
import java.util.Arrays;
import java.util.Collections;
import java.util.GregorianCalendar;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;

import com.esotericsoftware.kryo.Kryo;

import de.javakaffee.kryoserializers.ArraysAsListSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptyListSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptyMapSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptySetSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonListSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonMapSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonSetSerializer;
import de.javakaffee.kryoserializers.GregorianCalendarSerializer;
import de.javakaffee.kryoserializers.JdkProxySerializer;
import de.javakaffee.kryoserializers.SynchronizedCollectionsSerializer;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer;
import de.javakaffee.kryoserializers.jodatime.JodaLocalDateSerializer;
import de.javakaffee.kryoserializers.jodatime.JodaLocalDateTimeSerializer;

/**
 * Factory for Kryo, needed if used in combination with Spark (which seems to use a different version of Kryo).
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class KryoFactory {

	public static Kryo createKryoInstance() {
		Kryo kryo = new Kryo();
		
		// add custom serialisers to work with joda time (the kryo version that we get when including spark as a dependency cannot handle joda time by default)
		// see https://github.com/magro/kryo-serializers
		
		kryo.register( Arrays.asList( "" ).getClass(), new ArraysAsListSerializer() );
		kryo.register( Collections.EMPTY_LIST.getClass(), new CollectionsEmptyListSerializer() );
		kryo.register( Collections.EMPTY_MAP.getClass(), new CollectionsEmptyMapSerializer() );
		kryo.register( Collections.EMPTY_SET.getClass(), new CollectionsEmptySetSerializer() );
		kryo.register( Collections.singletonList( "" ).getClass(), new CollectionsSingletonListSerializer() );
		kryo.register( Collections.singleton( "" ).getClass(), new CollectionsSingletonSetSerializer() );
		kryo.register( Collections.singletonMap( "", "" ).getClass(), new CollectionsSingletonMapSerializer() );
		kryo.register( GregorianCalendar.class, new GregorianCalendarSerializer() );
		kryo.register( InvocationHandler.class, new JdkProxySerializer() );
		UnmodifiableCollectionsSerializer.registerSerializers( kryo );
		SynchronizedCollectionsSerializer.registerSerializers( kryo );

		// custom serializers for non-jdk libs
		kryo.register( DateTime.class, new JodaDateTimeSerializer() );
		kryo.register( LocalDate.class, new JodaLocalDateSerializer() );
		kryo.register( LocalDateTime.class, new JodaLocalDateTimeSerializer() );
		
		return kryo;
	}
	
}
