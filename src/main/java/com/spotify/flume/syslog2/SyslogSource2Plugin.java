package com.spotify.flume.syslog2;

import java.util.ArrayList;
import java.util.List;

import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.util.Pair;


/**
 * A loadable Flume plugin.
 *
 * This is the main entry point of the plugin.
**/
public class SyslogSource2Plugin {
	/**
	 * Return a list of available source builders.
	 *
	 * This is used by flume.conf.SourceFactoryImpl to instantiate
	 * source builders.
	**/
	public static List<Pair<String, SourceBuilder>> getSourceBuilders() {
		List<Pair<String, SourceBuilder>> ret = new ArrayList<Pair<String, SourceBuilder>>();
		
		ret.add(new Pair<String, SourceBuilder>(SyslogSourceBuilder.NAME, new SyslogSourceBuilder()));
		
		return ret;
	}
}
