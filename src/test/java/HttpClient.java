import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by YMY on 18/3/22.
 */
public class HttpClient {
    String data1 = "-----------汉室倾颓·讨伐董卓";
    String data = "《三国演义》反映了丰富的历史内容，人物名称、地理名称、主要事件与《三国志》基本相同。人物性格也是在《三国志》留下的固定形象基础上，才进行再发挥，进行夸张、美化、丑化等等，这也是历史演义小说的套路。《三国演义》一方面反映了真实的三国历史，照顾到读者希望了解真实历史的需要；另一方面，根据明朝社会的实际情况对三国人物进行了夸张、美化、丑化等等。\n" +
            "汉室倾颓·讨伐董卓\n" +
            "东汉末年，宦官当权，生灵涂炭，民不聊生。灵帝中平元年，张角兄弟发动黄巾起义，官军闻风丧胆。为抵抗黄巾，幽州太守刘焉出榜招兵。榜文前，刘备、关羽、张飞三兄弟萍水相逢。三人都有为国效力之心，而且志趣相投，于是桃园结为异姓兄弟，投靠刘焉。从军后刘、关、张显示出非凡的才能，一败黄巾于涿郡，二败黄巾于青州。不久，又救出被张角打败的董卓，但董卓见刘备是白身，并不答谢。张飞大怒，要斩董卓，被刘备劝住。刘关张与朱儁、孙坚进攻黄巾，大胜。朱儁、孙坚皆受封赏，只有刘备被冷落。很久之后，刘备才被封为定州中山府安喜县尉。到任四月，督邮来县巡视，借机索要贿赂。因刘备不从而欲存心陷害，张飞得知后怒鞭督邮，三人被迫弃去职位，投了刘恢。不久参加平定鱼阳之战，刘备因立功被任平原令，开始拥有一支人马。[1] \n" +
            "中平六年，汉灵帝死，少帝继位，为外戚大将军何进所制。十常侍诱杀何进，袁绍等领兵诛杀宦官，西凉刺史董卓趁机进兵京师、驱逐袁绍、灭丁原收吕布、废少帝立献帝，专权朝野，并毒死刘辩。司徒王允借寿诞之引，召集满朝公卿商议，曹操自告奋勇前往行刺，为董卓发觉，危急中献上自王允处借来的七星宝刀而脱身。[1] \n" +
            "曹操逃至中牟县为当时县令陈宫所获。陈宫义释曹操，并弃官随之离去。途经曹操之亲戚吕伯奢家时，因误会而杀害吕伯奢一家，并说出“宁教我负天下人，休教天下人负我”之语。陈宫愤怒，独自离开。曹操只身前往陈留，散尽家资招蓦兵马，亲友皆来相投，亦有了一支人马。曹操更写信给袁绍，并会齐中原豪杰。[1] \n" +
            "三国演义剧照\n" +
            "三国演义剧照(14张)\n" +
            "曹操、袁术等十八路诸侯与吕布对峙于汜水关，董卓派出华雄斩去十八镇诸侯多位上将，关羽自告奋勇却因自身的地位而被众诸侯所叱，唯曹操赏识人才，斟热酒令出战。酒尚温，关羽已斩华雄而归。随后，吕布骑赤兔马亲出虎牢关，袁绍亦派八路诸侯迎敌。众诸侯难敌吕布之勇，危难时候张飞救下公孙瓒而与吕布交手。因吕布奇勇，关羽、刘备先后出战，三人合力杀败吕布，吕布败退虎牢关。八路诸侯乘胜出击大获全胜，曹操暗中犒赏刘、关、张。[1] \n" +
            "董卓见吕布战败，盟军势大，烧洛阳，逼献帝迁都长安。盟军入洛阳，各起异心。孙坚在宫井中得到传国玉玺，率军返回江东。遇刘表阻拦，遂结怨。曹操与袁绍发生摩擦，去了扬州。盟军瓦解。接着军阀又开始火并。袁绍攻公孙瓒，被赵云所救。赵云本是袁绍部将，见袁绍无忠君救民之心，于是弃袁绍而投了公孙瓒，但不受重用。在江东，孙坚攻荆州，被刘表军士用乱箭射死。司徒王允在长安设下连环计，让董卓和吕布为争夺歌妓貂蝉而发生冲突，结果董卓被杀。董卓部将郭汜等反扑，杀害王允全家。[1] \n" +
            "群雄逐鹿·吕布覆灭\n" +
            "董卓死后，中原大乱，军阀混战之中，青州黄巾又起，曹操前往讨伐，编制青州兵，势力再次扩大。因曹操父曹嵩死于徐州，为报父仇，曹操借机征讨徐州，并下令屠城。徐州太守陶谦向北海孔融求救，孔融在平原找到刘备。刘备去公孙瓒处借了兵马，前往徐州助阵。[1] \n" +
            "另一方面，董卓旧将李傕、郭汜、樊稠、张济四人攻下长安，赶走吕布，逼死王允，再次挟天子。随后，马腾、韩遂联军勤王，马腾之子马超作战英勇，屡挫李郭军。但因为粮草供应困难，最终兵败退回凉州。[1] \n" +
            "在曹操报仇的同时，吕布、张邈采纳陈宫之言，袭了兖州，曹操被迫回战吕布。濮阳城中一战，曹操险些丧命。而刘备处，陶谦三让徐州，使曹操费力征讨不得的徐州为刘备唾手而得。[1] \n" +
            "吕布\n" +
            "吕布\n" +
            "长安处李傕、郭汜发生内讧，曹操入洛阳救驾，借机将献帝劫至许昌，开始 “挟天子以令诸侯”，大权独揽。与此同时，江东孙策利用亡父孙坚留下的传国玉玺，向袁术借了兵马，逐渐平定江东六郡八十一州，奠立了日后三分天下有其一的吴国基业。袁术得了玉玺，即刻称帝。[1] \n" +
            "吕布兵败投了刘备，却趁刘备征伐袁术之机夺了徐州。刘备暂居小沛。在刘备与袁术两家求助之际，吕布辕门射戟救了刘备。不久刘备又为吕布所迫，投了曹操。曹操先后三次征伐张绣而未果，张绣自行投降。[1] \n" +
            "建安三年（公元198年），曹操讨伐吕布，苦战日久未果。而吕布却因内患陈登，且惑于妻妾之言，终为曹操所擒。白门楼上，刘备以丁原、董卓的缘由将吕布致死。曹操也怒斩高顺，泪别陈宫，义降张辽。吕布既定，曹操势力进一步扩大。[1] \n" +
            "内阁密诏·千里独行\n" +
            "朝延上，曹操作威作福，许田打围之时对献帝无礼招致忠臣愤怒。皇帝密召国舅董承入宫，授以衣带诏，教图曹操。西凉马腾与刘备最终亦参与此谋。刘备参与后为避曹操嫌疑，在园中种菜却为曹操叫去赏梅。\n" +
            "关羽\n" +
            "关羽\n" +
            "曹操煮酒论英雄令刘备吃惊，以畏雷之说巧妙掩饰。不久刘备即借剿灭袁术之机脱身，袁术与刘备交战，大败。于是袁术前往投降袁绍，于路中被劫，最终渴死。其部将割其首并玉玺一同交与曹操。[1] \n" +
            "皇宫内，衣带诏事发，董承等皆遇害。曹操派兵征讨刘备，刘备大败而与张飞失散，投了袁绍。关羽被困下邳，曹操爱其才，遣张辽说降，为保刘备家眷，关羽与曹操约法三章而降。在曹操处，关羽受到厚待，得了吕布所骑赤兔马。[1] \n" +
            "在刘备建议下，袁绍起兵与曹操交战于白马，关羽斩颜良诛文丑，解了曹操白马之围。因得知刘备在袁绍处，关羽离开许都，前往河北。临行时挂印封金，令曹操既憾又赞。关羽一路，千里走单骑，过五关斩六将，几经周折，最终于张飞、刘备聚于古城。又因公孙瓒兵败自焚，赵云独自一人云游四海与刘备相遇，自此终身为刘备爱将，情义比桃园。[1] \n" +
            "官渡之战·大破袁绍\n" +
            "建安四年（公元199年），江东孙策为部下家客所害，身受重伤，更因受于吉之气而身亡。孙权坐领江东，承父兄基业，与曹操修好。[1] \n" +
            "袁绍得知后起大军七十万伐曹操，曹操起七万兵相迎，战于官渡（此时为建安五年，公元200年）。曹操几经挫折却始终坚持。袁绍谋士许攸进言而不为所用，因而来投曹操，并献火烧乌巢之计。一夜大火，袁绍势力大衰。仓亭处曹操再破袁绍，最终袁绍吐血而亡。刘备趁火打劫，自汝南起兵攻曹，却为曹操打败，投了荆州刘表，驻守新野。大小数战，曹操终攻克冀州，然而郭嘉病故。郭嘉临终时献密计，使曹操短期平定了辽东，统治北方。[1] \n" +
            "三顾茅庐·荆襄之变\n" +
            "建安十二年（公元207年），荆州处，因刘表次子娘舅蔡瑁与刘备争权，而两番设计陷害刘备。刘备跃马檀溪，大难不死，行至襄阳境内水镜庄上，得水镜先生司马徽点拔，闻卧龙、凤雏之说。次日路遇毛遂自荐的徐庶，即刘备军中的第一位军师。曹军进犯，徐庶大败曹军，展露的大才为刘备大开眼界。曹操以徐庶之母为要挟将徐庶逼进许都，徐庶临行时，向刘备推荐了诸葛亮，即卧龙先生，并许下了终生不为曹操献计之誓。[1] \n" +
            "自建安十二年秋至建安十三年（公元208年）春，刘备来到南阳三顾茅庐，寻访诸葛亮。诸葛亮大为感动，在卧龙岗为刘备分析了天下形势，道破天时、地利、人和之玄机，最终出山辅佐刘备，成为一代贤相。[1] \n" +
            "两次火攻南阳的博望坡和新野城，诸葛亮大破曹军。刘表病逝，蔡瑁暗里拥立幼子刘琮，并将荆襄九郡献了曹操。曹操83万大军下追赶刘备。刘备被迫携民渡江，饱经挫折屈辱。赵云为救幼主刘禅，屡次单骑闯入曹操军营，闯下一世英明。同时张飞大喝长坂桥，又使曹营许多将士从此闻风丧胆。在关羽、诸葛亮与刘表长子刘琦的接引下，刘备入了江夏。[1] \n" +
            "赤壁之战·火烧曹兵\n" +
            "曹操书信至江东，威胁孙权投降，共擒刘备。孙权派鲁肃过江\n" +
            "曹操\n" +
            "曹操\n" +
            "探虚实。诸葛亮借机入东吴，舌战群儒，智激周瑜，终于使得孙权决计破曹操。在柴桑，周瑜起五万兵，与曹操83万大军隔江对峙。水上交战，曹军失利，蒋干去东吴军营劝降周瑜，却中周瑜反间计，误盗周瑜伪造的书信而使曹操误杀蔡瑁、张允——曹营中仅有的两名深懂水战之将。在东吴军营，周瑜深恨诸葛亮之才不能为东吴所用，使诸葛瑾劝降诸葛亮未果，即起心害之。两番斗智，均为诸葛亮胜出。第二次的“草船借箭”，更是使周瑜自叹不如。[1] \n" +
            "周瑜与诸葛亮不谋而合，定下火攻之策。为火烧曹军战船，老将黄盖行苦肉计，惨被棒疮；阚泽去曹营献诈降书，又遭到曹操恐吓。蒋干再次过江，周瑜安排江东隐居的凤雏先生庞统随之回曹营，巧授连环计，教曹操将战船全部连接。[1] \n" +
            "诸葛亮借机去七星坛祭风，实则逃离东吴，回到江夏，智算华容。三江口处，黄盖前往诈降。是夜东南风大起，曹操水寨一片火海，一夜之间，曹操83万大军所剩无几。逃命过程中又遭到赵云、张飞与关羽三次堵截。最终华容道关羽义释曹操，使得曹操逃至南郡。败阵的曹操，对郭嘉怀念不已。[1] \n";

    @Test
    public void testGet(){
        for(int i = 0; i < 1000;i ++){
            requestByGetMethod("id="+i+"&data="+1);
        }
    }
    @Test
    public void testPost(){
        for(int i = 0; i < 1000;i ++){
            requestByPostMethod("id="+i+"&data="+data);
        }
    }
    /**
     * 通过GET方式发起http请求
     */

    public void requestByGetMethod(String args){
        //创建默认的httpClient实例
        CloseableHttpClient httpClient = getHttpClient();
        try {
            //用get方法发送http请求
            HttpGet get = new HttpGet("http://192.168.200.8:8011/logagent?"+args);
            System.out.println("执行get请求:...."+get.getURI());
            CloseableHttpResponse httpResponse = null;
            //发送get请求
            httpResponse = httpClient.execute(get);
            try{
                //response实体
                HttpEntity entity = httpResponse.getEntity();
                if (null != entity){
                    System.out.println("响应状态码:"+ httpResponse.getStatusLine());
                    System.out.println("-------------------------------------------------");
                    System.out.println("响应内容:" + EntityUtils.toString(entity));
                    System.out.println("-------------------------------------------------");
                }
            }
            finally{
                httpResponse.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally{
            try{
                closeHttpClient(httpClient);
            } catch (IOException e){
                e.printStackTrace();
            }
        }

    }


    /**
     * POST方式发起http请求
     */
    public void requestByPostMethod(String data){
        CloseableHttpClient httpClient = getHttpClient();
        try {
            HttpPost post = new HttpPost("http://192.168.200.8:8011/logagent");          //这里用上本机的某个工程做测试
            //创建参数列表
            List<NameValuePair> list = new ArrayList<NameValuePair>();
            list.add(new BasicNameValuePair("topic", "topic_1"));
            list.add(new BasicNameValuePair("j_username", "admin"));
            list.add(new BasicNameValuePair("j_password", "admin"));
            list.add(new BasicNameValuePair("data", data));
            //url格式编码
            UrlEncodedFormEntity uefEntity = new UrlEncodedFormEntity(list,"UTF-8");
            post.setEntity(uefEntity);
            System.out.println("POST 请求...." + post.getURI());
            //执行请求
            CloseableHttpResponse httpResponse = httpClient.execute(post);
            try{
                HttpEntity entity = httpResponse.getEntity();
                if (null != entity){
                    System.out.println("-------------------------------------------------------");
                    System.out.println(EntityUtils.toString(uefEntity));
                    System.out.println("-------------------------------------------------------");
                }
            } finally{
                httpResponse.close();
            }

        } catch( UnsupportedEncodingException e){
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally{
            try{
                closeHttpClient(httpClient);
            } catch(Exception e){
                e.printStackTrace();
            }
        }

    }

    private CloseableHttpClient getHttpClient(){
        return HttpClients.createDefault();
    }

    private void closeHttpClient(CloseableHttpClient client) throws IOException{
        if (client != null){
            client.close();
        }
    }
}
