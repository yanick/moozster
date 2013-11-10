package moozter;

use Dancer ':syntax';

use Net::Hadoop::WebHDFS;
use Path::Tiny;
use Web::Query;
use REST::Client;

our $VERSION = '0.1';

get '/' => sub {
    template 'index';
};

get '/workflow/:workflow' => sub {
    my $workflow = param('workflow');
    
    return template '/workflow', {
        workflow => $workflow
    };

};

get '/job/:id' => sub {
    my $client = REST::Client->new;
    $client->setHost( 'http://'.config->{hadoop_host} . ':11000' );

    $client->GET( '/oozie/v1/job/' . param('id') . '?show=info' );
    return $client->responseContent;
};

post '/workflow/:workflow/launch' => sub {
    my $workflow = param('workflow');

    my $client = REST::Client->new;
    $client->setHost( 'http://'.config->{hadoop_host} . ':11000' );
    $client->addHeader( 'Content-Type' => 'application/xml;charset=UTF-8' );

    my $host = config->{hadoop_host};
    my $path = config->{workspace_root} . '/' . $workflow . '/';

    $client->POST( '/oozie/v1/jobs?action=start', <<"END"
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>user.name</name>
        <value>hue</value>
    </property>
    <property>
        <name>oozie.wf.application.path</name>
        <value>hdfs://$path</value>
    </property>
</configuration>
END
    );

    return $client->responseContent;
        
};

get '/workflow/:workflow/graph' => sub {
    my $workflow = param('workflow');

    return workflow_to_graph( get_graph_from_hdfs( $workflow ) );
};

sub get_graph_from_hdfs {
    return Net::Hadoop::WebHDFS->new( 'host' => config->{hadoop_host} )
        ->read( path( config->{workspace_root}, shift @_, 'workflow.xml' ) )
        || die "'workflow.xml' not found";
}

sub workflow_to_graph {
    my $q = Web::Query->new_from_html( shift );

    my %graph;

    $q->find( 'start' )->each(sub{
        push @{ $graph{START} }, $_[1]->attr('to');
    });

    $q->find( 'end' )->each(sub{
        $graph{$_[1]->attr('name')} = [];
    });

    $q->find('action')->each(sub{
        for my $next (qw/ ok error /) {
            my $next_node = $_[1]->find($next)->attr('to') or next;
            push @{ $graph{$_[1]->attr('name')} }, $next_node;
        }
    });

    $q->find('fork')->each(sub{
        my $name = $_[1]->attr('name');
        $_[1]->find('path')->each(sub{
            push @{$graph{$name}}, $_[1]->attr('start');
        });
    });

    $q->find('join')->each(sub{
            push @{$graph{$_[1]->attr('name')}}, $_[1]->attr('to');
    });

    $q->find('decision')->each(sub{
        my $name = $_[1]->attr('name');
        $_[1]->find('case,default')->each(sub{
            my $next_node = $_[1]->attr('to') or next;
            push @{ $graph{$_[1]->attr('name')} }, $next_node;
        });

    });

    # just make sure all nodes are present as keys
    $graph{$_} ||= [] for map { @$_ } values %graph;

    return \%graph;
}





true;
