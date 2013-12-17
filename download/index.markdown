--- 
layout: inner_simple
title: Download
---

<p style="font-size: 50px;margin-bottom:50px" class="text-center">Download</p>

<table class="table table-bordered table-striped">
	<thead>
		<tr>
			<th></th>
			<th class="text-center">Hadoop 1.2.X</th>
			<th class="text-center">Hadoop 2 (YARN)</th>
		</tr>
	</thead>
	<tbody>
		<tr>
			<td class="text-center"><b>Binary</b></td>
			<td class="text-center"><a class="btn btn-primary btn-lg" target="_blanc" href="http://dopa.dima.tu-berlin.de/bin/stratosphere-0.4-SNAPSHOT.tgz">
      <i class="icon-download"> </i>Download</a>
			<p style="text-align: right; font-style: italic;"><a href="/docs/gettingstarted.html">How to install?</a></p>
			</td>
			<td class="text-center"><a class="btn btn-primary btn-lg" target="_blanc" href="http://dopa.dima.tu-berlin.de/bin/stratosphere-0.4-hadoop2-SNAPSHOT.tgz">
      <i class="icon-download"> </i>Download</a>
			<p style="text-align: right; font-style: italic;"><a href="/docs/gettingstarted.html">How to install?</a></p>
			</td>
		</tr>
		<tr>
			<td class="text-center"><b>Maven Dependencies</b><br /><br />adding stratosphere to your own maven project</td>
			<td><pre class="prettyprint" style="padding-left:1em">
&lt;project&gt;
  ...
  &lt;dependencies&gt;
    &lt;dependency&gt;
      &lt;groupId&gt;eu.stratosphere&lt;/groupId&gt;
      &lt;artifactId&gt;stratosphere&lt;/artifactId&gt;
      &lt;version&gt;0.4-SNAPSHOT&lt;/version&gt;
      &lt;type&gt;pom&lt;/version&gt;
    &lt;/dependency&gt;
  &lt;/dependencies&gt;
&lt;/project&gt;
</pre>
			</td>
			<td><pre class="prettyprint" style="padding-left:1em">
&lt;project&gt;
  ...
  &lt;dependencies&gt;
    &lt;dependency&gt;
      &lt;groupId&gt;eu.stratosphere&lt;/groupId&gt;
      &lt;artifactId&gt;stratosphere&lt;/artifactId&gt;
      &lt;version&gt;0.4-hadoop2-SNAPSHOT&lt;/version&gt;
      &lt;type&gt;pom&lt;/version&gt;
    &lt;/dependency&gt;
  &lt;/dependencies&gt;
&lt;/project&gt;
</pre>
			</td>
		</tr>
		<tr>
			<td class="text-center"><b>Quickstart</b></td>
			<td class="text-center" colspan="2">
				Java: <pre class="prettyprint" style="padding-left:1em">curl https://raw.github.com/stratosphere/stratosphere-quickstart/master/quickstart.sh | bash</pre>
				Scala: <pre class="prettyprint" style="padding-left:1em">curl https://raw.github.com/stratosphere/stratosphere-quickstart/master/quickstart-scala.sh | bash</pre>
				<p style="text-align: right; font-style: italic;">For more information see <a href="/quickstart/">Quickstart</a></p>
			</td>
		</tr>
		<tr>
			<td class="text-center"><b>Virtual Machine</b></td>
			<td class="text-center" colspan="2">
				<a class="btn btn-primary btn-lg" target="_blanc" href="http://dev.stratosphere.eu/vm/">
      				<i class="icon-download"> </i>Download</a>
				<p style="text-align: right; font-style: italic;">Download the files and import to VirtualBox</a></p>
			</td>
		</tr>
		<tr>
			<td class="text-center"><b>Vagrant</b></td>
			<td class="text-center" colspan="2">
				<a class="btn btn-primary btn-lg" target="_blanc" href="http://dev.stratosphere.eu/vm/Vagrantfile">
      				<i class="icon-download"> </i>Download</a>
				<p style="font-style: italic; margin-top: 5px">Copy Vagrant file to own directory, open shell and type:
				<pre class="prettyprint" style="padding-left:1em; text-align: left; max-width: 400px; margin: 0 auto">
vagrant up
vagrant ssh</pre></p>
			</td>
		</tr>
		<!--<tr>
			<td class="text-center"><b>Debian Package</b></td>
			<td colspan="2">
				<pre class="prettyprint" style="padding-left:1em">
# vim /etc/apt/sources.list.d/stratosphere.list
deb http://dev.stratosphere.eu/repo/binary precise main</pre>
				<pre class="prettyprint" style="padding-left:1em">
# apt-get update
apt-get install stratosphere-dist</pre>
				<p style="text-align: right; font-style: italic;">For more information see <a href="/quickstart/">Quickstart</a></p>
			</td>
		</tr>-->
		<tr>
			<td class="text-center"><b>Source</b><br /><br />Compile stratosphere on your own machine.</td>
			<td class="text-center">
				<pre class="prettyprint" style="padding-left:1em; text-align: left">
git clone https://github.com/stratosphere/stratosphere.git
cd stratosphere
mvn clean package -DskipTests</pre>
			</td>
			<td class="text-center">
				<pre class="prettyprint" style="padding-left:1em; text-align: left">
git clone https://github.com/stratosphere/stratosphere.git
cd stratosphere
mvn clean package -DskipTests -Dhadoop.profile=2</pre>
			</td>
		</tr>
	</tbody>
</table>


<!--
<div class="bs-docs-grid">
<div class="row">
	<div class="col-md-2"></div>
	<div class="col-md-5 text-center"><h3>Hadoop 1.2.X</h3></div>
	<div class="col-md-5text-center"><h3>Hadoop 2 (YARN)</h3></div>
</div>
<div class="row">
	<div class="col-md-2"><b>Binary</b></div>
	<div class="col-md-5 text-center">
	<a class="btn btn-primary btn-lg" target="_blanc" href="http://dopa.dima.tu-berlin.de/bin/stratosphere-0.4-SNAPSHOT.tgz"><i class="icon-download"> </i>Download</a>
	<p style="text-align: right; font-style: italic;"><a href="/docs/gettingstarted.html">How to install?</a></p>
	</div>
	<div class="col-md-5 text-center">
	<a class="btn btn-primary btn-lg" target="_blanc" href="http://dopa.dima.tu-berlin.de/bin/stratosphere-0.4-hadoop2-SNAPSHOT.tgz"><i class="icon-download"> </i>Download</a>
	<p style="text-align: right; font-style: italic;"><a href="/docs/gettingstarted.html">How to install?</a></p>
	</div>
</div>
<div class="row">
	<div class="col-md-2"><b>Maven Dependencies</b><p>adding stratosphere to your own maven project</div>
	<div class="col-md-5">
	<pre class="prettyprint" style="padding-left:1em">
&lt;project&gt;
  ...
  &lt;dependencies&gt;
    &lt;dependency&gt;
      &lt;groupId&gt;eu.stratosphere&lt;/groupId&gt;
      &lt;artifactId&gt;todo&lt;/artifactId&gt;
      &lt;version&gt;todo&lt;/version&gt;
      &lt;type&gt;bar&lt;/type&gt;
      &lt;scope&gt;runtime&lt;/scope&gt;
    &lt;/dependency&gt;
  &lt;/dependencies&gt;
&lt;/project&gt;
	</pre>
	</div>
	<div class="col-md-5">
	<pre class="prettyprint" style="padding-left:1em">
&lt;project&gt;
  ...
  &lt;dependencies&gt;
    &lt;dependency&gt;
      &lt;groupId&gt;eu.stratosphere&lt;/groupId&gt;
      &lt;artifactId&gt;todo&lt;/artifactId&gt;
      &lt;version&gt;todo&lt;/version&gt;
      &lt;type&gt;bar&lt;/type&gt;
      &lt;scope&gt;runtime&lt;/scope&gt;
    &lt;/dependency&gt;
  &lt;/dependencies&gt;
&lt;/project&gt;
	</pre>
	</div>
</div>
<div class="row">
	<div class="col-md-2"><b>Quickstart</b></div>
	<div class="col-md-10 text-center">
		Java: <pre class="prettyprint" style="padding-left:1em">curl https://raw.github.com/stratosphere/stratosphere-quickstart/master/quickstart.sh | bash</pre>
		Scala: <pre class="prettyprint" style="padding-left:1em">curl https://raw.github.com/stratosphere/stratosphere-quickstart/master/quickstart.sh | bash</pre>
		<p style="text-align: right; font-style: italic;">For more information see <a href="/quickstart/">Quickstart</a></p>
	</div>
</div>
</div>
-->
