import org.apache.spark.SparkContext._

case class NameRep(rep: Int, name: String)

def extractNameRep(row: String) = {
  // Matches Reputation and Display Name.
  val repNameRegex = """Reputation="(\d+)".+DisplayName="([A-Za-z0-9_ ]*)"""".r;
  val nameRep = repNameRegex findFirstMatchIn (row) match {
    case Some(m) => new NameRep(m.group(1).toInt, m.group(2))
    // If match is not found or line returns None
    case _ => None
  }
}

val list = List("  <row Id=\"3\" Reputation=\"101\" CreationDate=\"2012-03-06T18:39:11.967\" DisplayName=\"Nick Craver\" LastAccessDate=\"2013-09-24T12:47:57.667\" WebsiteUrl=\"http://nickcraver.com/blog/\" Location=\"Winston-Salem, NC\" AboutMe=\"&lt;p&gt;&lt;a href=&quot;http://blog.stackoverflow.com/2011/01/welcome-valued-associate-nick-craver/&quot;&gt;Stack Overflow Valued Associate&lt;/a&gt;&lt;/p&gt;&#xA;&#xA;&lt;p&gt;Twitter: &lt;a href=&quot;http://twitter.com/Nick_Craver&quot; rel=&quot;nofollow&quot;&gt;@Nick_Craver&lt;/a&gt;&lt;br&gt;&#xA;Public CV: &lt;a href=&quot;http://careers.stackoverflow.com/ncraver&quot;&gt;On StackOverflow Careers&lt;/a&gt;&lt;br&gt;&#xA;Flickr: &lt;a href=&quot;http://www.flickr.com/photos/nickcraver/&quot; rel=&quot;nofollow&quot;&gt;Photos on new house progress, where I work, etc.&lt;/a&gt;&lt;/p&gt;&#xA;&#xA;&lt;p&gt;I mostly live in the C#/Linq2SQL/Raw SQL world, but at night I secretly masquerade as someone who pretends to know &lt;a href=&quot;http://stackoverflow.com/questions/tagged/javascript&quot;&gt;JavaScript&lt;/a&gt; &amp;amp; &lt;a href=&quot;http://stackoverflow.com/questions/tagged/jquery&quot;&gt;jQuery&lt;/a&gt;.&lt;br&gt;&#xA;&lt;sub&gt;Disclaimer: I have &lt;em&gt;no idea&lt;/em&gt; what I'm talking about, all my answers are guesses!&lt;/sub&gt;&lt;/p&gt;&#xA;\" Views=\"5\" UpVotes=\"1\" DownVotes=\"0\" ProfileImageUrl=\"http://i.stack.imgur.com/nGCYr.jpg\" Age=\"29\" AccountId=\"7598\" />",
  "  <row Id=\"8\" Reputation=\"731\" CreationDate=\"2012-03-06T18:46:51.213\" DisplayName=\"Opt\" LastAccessDate=\"2014-09-04T04:55:35.673\" Views=\"32\" UpVotes=\"44\" DownVotes=\"0\" AccountId=\"44900\" />")

val output = list.flatMap(line => line.split(" ")).map(word => (word,1)).red

